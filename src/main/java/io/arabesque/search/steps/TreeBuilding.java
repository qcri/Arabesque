package io.arabesque.search.steps;

import com.koloboke.collect.IntIterator;
import io.arabesque.QfragRunner;
import io.arabesque.conf.Configuration;
import io.arabesque.conf.SparkConfiguration;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.search.trees.Domain;
import io.arabesque.search.trees.SearchDataTree;
import io.arabesque.utils.ThreadOutput;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;
import org.mortbay.log.Log;
import scala.Tuple2;

import java.util.*;

import static io.arabesque.search.steps.QueryGraph.STAR_LABEL;
import static io.arabesque.utils.Hashing.murmur3_32;

//import net.openhft.koloboke.collect.IntIterator;

public class TreeBuilding
        implements Function2<Integer, Iterator<Tuple2<Integer, SearchDataTree>>,
            Iterator<Tuple2<Integer, SearchDataTree>>> {

    private static final Logger LOG = Logger.getLogger(TreeBuilding.class);

    private final int totalPartitions;
    private Broadcast<SparkConfiguration> configBC;
    private Broadcast<QueryGraph> queryGraphBC;
    private final boolean injective;

    private double outlierPct;
    private final int minMatches;

    // accumulators
    private Map<String, CollectionAccumulator<Long>> accums;
    private long init_Start_Time = 0;
    private long init_Finish_Time = 0;
    private long computation_Start_Time = 0;
    private long computation_Finish_Time = 0;

    public TreeBuilding(int totalPartitions, Broadcast<SparkConfiguration> configBC, Broadcast<QueryGraph> queryGraphBC, Map<String, CollectionAccumulator<Long>> _accums) {
        super();

        init_Start_Time = System.currentTimeMillis();

        configBC.value().initialize();
        Configuration conf = Configuration.get();

        String log_level = conf.getLogLevel();
        LOG.fatal("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));

        this.totalPartitions = totalPartitions;
        this.configBC = configBC;
        this.queryGraphBC = queryGraphBC;

        this.minMatches = conf.getInteger(conf.SEARCH_OUTLIERS_MIN_MATCHES, conf.SEARCH_OUTLIERS_MIN_MATCHES_DEFAULT).intValue();

        try {
            this.outlierPct = conf.getDouble(conf.SEARCH_OUTLIERS_PCT, conf.SEARCH_OUTLIERS_PCT_DEFAULT);
        } catch (ClassCastException e){
            if (e.toString().contains("java.lang.Integer cannot be cast to java.lang.Double")){
                int outlierPctInt = conf.getInteger(conf.SEARCH_OUTLIERS_PCT, (int) conf.SEARCH_OUTLIERS_PCT_DEFAULT);
                this.outlierPct = (double) outlierPctInt;
            }
            else {
                throw e;
            }
        }

        this.injective = conf.getBoolean(conf.SEARCH_INJECTIVE, conf.SEARCH_INJECTIVE_DEFAULT);

        this.accums = _accums;
    }

    @Override
    public Iterator<Tuple2<Integer, SearchDataTree>> call(Integer partitionId, Iterator<Tuple2<Integer, SearchDataTree>> v2) {

        Configuration conf = configBC.value();
        conf.initialize();

        UnsafeCSRGraphSearch dataGraph = Configuration.get().getSearchMainGraph();
        QueryGraph queryGraph = queryGraphBC.getValue();

        // get the initialization finish time stamp
        init_Finish_Time = System.currentTimeMillis();
        // get the computation runtime stamp
        computation_Start_Time = System.currentTimeMillis();

        PriorityQueue<SearchDataTreeCost> heavyTrees;
        IntIterator searchExtensionsIterator = dataGraph.createNeighborhoodSearchIterator();
        IntArrayList reusableExtensions = new IntArrayList();

        ThreadOutput out = ThreadOutputHandler.createThreadOutput(partitionId, false);

        CrossEdgeMatchingLogic crossEdgeMatchingLogic = new CrossEdgeMatchingLogic(dataGraph, queryGraph, out);

        IntArrayList candidates = new IntArrayList();

        // ###### Create initial list of candidates ######

        int rootsCount = pickLocalCandidates(partitionId, dataGraph, queryGraph, candidates);
        IntIterator rootsIterator = candidates.iterator();

        if (rootsCount == 0) {
            return (new ArrayList<Tuple2<Integer, SearchDataTree>>()).iterator();
        }

        SearchDataTree currentSearchDataTree = getNextDataTree(rootsIterator, queryGraph, null);
        if (currentSearchDataTree == null) {
            return (new ArrayList<Tuple2<Integer, SearchDataTree>>()).iterator();
        }

        int topk = Math.max(1, (int) ((double) rootsCount * outlierPct));
        heavyTrees = new PriorityQueue<>(topk);

        LOG.info("QFrag: rootsCount " + rootsCount + " topk " + topk + " minMatches " + minMatches);

        // ###### Iterate across all candidates. for each build the candidate (data) tree and find matches ######

        while (true) {

            // builds the search data tree based on the query graph
            expand(currentSearchDataTree, dataGraph, queryGraph, reusableExtensions, searchExtensionsIterator);

            long matchingCost = currentSearchDataTree.getMatchingCost();

            boolean matchCurrentTree = true;

            if (outlierPct > 0
                    && queryGraph.hasCrossEdges()
                    && matchingCost > minMatches
                    ) {

                if(heavyTrees.size() < topk){
                    heavyTrees.add(new SearchDataTreeCost(currentSearchDataTree,matchingCost));
                    matchCurrentTree = false;
                    // set to null so that getNextDataTree creates a new data tree instead of reusing the one in the queue
                    currentSearchDataTree = null;
                } else {
                    if (heavyTrees.peek().cost < matchingCost){
                        SearchDataTreeCost polledDataTree = heavyTrees.poll();

                        SearchDataTree tmp = polledDataTree.dataTree;
                        polledDataTree.dataTree = currentSearchDataTree;
                        currentSearchDataTree = tmp;

                        polledDataTree.cost = matchingCost;
                        heavyTrees.add(polledDataTree);
                    }
                }
            }

            if (matchCurrentTree) {
                // finished with the currentSearchDataTree, proceed to merging
                crossEdgeMatchingLogic.matchCrossEdges(currentSearchDataTree);
            }

            // switch to new data tree if available
            do{
                // the following resets currentSearchDataTree
                currentSearchDataTree = getNextDataTree(rootsIterator, queryGraph, currentSearchDataTree);
                if (currentSearchDataTree == null) {
                    // finished all the work we have locally
                    ThreadOutputHandler.closeThreadOutput(out);
                    return sendOutliers(partitionId, heavyTrees);
                }
            } while (currentSearchDataTree.size() == 0);
        }
    }

    private int pickLocalCandidates(int partitionId, UnsafeCSRGraphSearch dataGraph, QueryGraph queryGraph, IntArrayList candidates){
        int[] tmp = new int[1];

        IntArrayList startCandidates = queryGraph.getRootMatchingVertices();

        if(startCandidates != null) {
            for (int i = 0; i < startCandidates.size(); i++) {
                int candidate = startCandidates.getUnchecked(i);
                tmp[0] = candidate;

                int res = Math.abs(murmur3_32(tmp)) % totalPartitions;
                if (res == partitionId) {
                    candidates.add(candidate);
                }
            }
        } else {
            // the root is not labeled
            for (int candidate = 0; candidate < dataGraph.getNumberVertices(); candidate++){
                tmp[0] = candidate;

                int res = Math.abs(murmur3_32(tmp)) % totalPartitions;
                if (res == partitionId) {
                    candidates.add(candidate);
                }
            }
        }
        return candidates.size();
    }

    private int expand(SearchDataTree searchDataTree, UnsafeCSRGraphSearch dataGraph, QueryGraph queryGraph,
                       IntArrayList reusableExtensions, IntIterator searchExtensionsIterator){

        int totalExtensionsVisited = 0;

        // each loop adds one domain to the searchDataTree
        for(int destDFSPos = 1; destDFSPos < queryGraph.getNumberVertices(); destDFSPos++){
            int sourceDFSPos = queryGraph.getChildToFatherDFS(destDFSPos);
            int destinationLabel = queryGraph.getDestinationLabel(destDFSPos);
            int destinationDegree = queryGraph.getDestinationDegree(destDFSPos);
            IntArrayList edgeLabels = queryGraph.getEdgeLabels(destDFSPos);

            if(sourceDFSPos == 0){

                int sourceDataVertexId = searchDataTree.rootDataVertexId;

                // find neighbors of root with right labels
                totalExtensionsVisited += addExtensions(searchDataTree, dataGraph, queryGraph, sourceDataVertexId,
                        destDFSPos, destinationLabel, destinationDegree, edgeLabels, reusableExtensions, searchExtensionsIterator);
            } else {
                Domain.DomainIterator iterator = searchDataTree.getDomainIterator(sourceDFSPos);
                while (iterator.hasNext()) {

                    int sourceDataVertexId = iterator.nextInt();

                    // find neighbors of sourceDataVertexId with right labels
                    totalExtensionsVisited += addExtensions(searchDataTree, dataGraph, queryGraph, sourceDataVertexId,
                            destDFSPos, destinationLabel, destinationDegree, edgeLabels, reusableExtensions, searchExtensionsIterator);
                }
            }
        }
        return totalExtensionsVisited;
    }

    private int addExtensions(SearchDataTree searchDataTree, UnsafeCSRGraphSearch dataGraph, QueryGraph queryGraph, int sourceDataVertexId,
                              int destDFSPos, int destinationLabel, int destinationDegree, IntArrayList edgeLabels,
                              IntArrayList reusableExtensions, IntIterator searchExtensionsIterator){

        int visitedExtensions = 0;
        reusableExtensions.clear();


        if (searchDataTree.isExtended(sourceDataVertexId,destDFSPos)){
            return 0;
        }

        dataGraph.setIteratorForNeighborsWithLabel(sourceDataVertexId,
                destinationLabel,
                searchExtensionsIterator);

        if (searchExtensionsIterator == null) {
            return 0;
        }

        // iterate over all neighbors of sourceDataVertexId with label destinationLabel
        while (searchExtensionsIterator.hasNext()) {
            int extension = searchExtensionsIterator.nextInt();
            visitedExtensions++;
            int dataVertexDegree = dataGraph.getNeighborhoodSizeWithLabel(extension, -1);
            if (dataVertexDegree < destinationDegree){
                continue;
            }

            // check that all edge labels are matching
            if (edgeLabels != null) {
                // If we have the star label, then we know we are neighbors already.
                if (!queryGraph.has_star_label_on_edge() || !edgeLabels.contains(STAR_LABEL)) {
                    if (!dataGraph.hasEdgesWithLabels(sourceDataVertexId, extension, edgeLabels)) {
                        //Skip this candidate, we are not neighbors.
                        continue;
                    }
                }
            }

            reusableExtensions.add(extension);
        }

        if(reusableExtensions.size() > 0){
            // add candidate subregion to data tree
            searchDataTree.addCandidateSubregion(sourceDataVertexId, destDFSPos, reusableExtensions);
        }

        return visitedExtensions;
    }


    private SearchDataTree getNextDataTree(IntIterator rootsIterator, QueryGraph queryGraph, SearchDataTree currentSearchDataTree){
        if(!rootsIterator.hasNext()){
            return null;
        }
        int candidate = rootsIterator.nextInt();
        if (currentSearchDataTree == null){
            return new SearchDataTree(candidate, queryGraph, totalPartitions, injective);
        } else {
            currentSearchDataTree.resetTo(candidate);
            return currentSearchDataTree;
        }
    }

    private class SearchDataTreeCost implements Comparable<SearchDataTreeCost>{
        private SearchDataTree dataTree;
        private long cost;

        private SearchDataTreeCost(SearchDataTree dataTree, long cost){
            this.dataTree = dataTree;
            this.cost = cost;
        }

        @Override
        public int compareTo(SearchDataTreeCost other) {
            if (this.cost < other.cost){
                return -1;
            }
            if (this.cost > other.cost){
                return 1;
            }
            return 0;
        }
    }

    private Iterator<Tuple2<Integer, SearchDataTree>> sendOutliers(int partitionId, PriorityQueue<SearchDataTreeCost> heavyTrees){
        Random rcvGenerator = new Random(Thread.currentThread().getId());

        ArrayList<Tuple2<Integer, SearchDataTree>> output = new ArrayList<>();

        int currPartition = rcvGenerator.nextInt(totalPartitions);

        LOG.info("QFrag: Sending " + heavyTrees.size() + " heavy trees");

        for (SearchDataTreeCost dataTreeCost : heavyTrees) {

            SearchDataTree dataTree = dataTreeCost.dataTree;

            if(dataTree.isRootPruned()){
                continue;
            }

            ArrayList<SearchDataTree> splits = dataTree.split();

            if(splits == null || splits.isEmpty()){
                output.add(new Tuple2<>(partitionId,dataTree));
            } else {

                for (int currSplit = 0; currSplit < splits.size(); currSplit++) {
                    output.add(new Tuple2<>(currPartition % totalPartitions, splits.get(currSplit)));
                    currPartition ++;
                }
            }
        }

        // This is supposed to be the end of tree building step
        // So flush the accumulators
        flushAccumulators();

        return output.iterator();
    }

    private void accumulate(Long value, CollectionAccumulator<Long> _accum) {
        _accum.add( value );
    }

    private void flushAccumulators() {
        // Now calculate the computation runtime
        computation_Finish_Time = System.currentTimeMillis();

        accumulate(init_Start_Time, accums.get(QfragRunner.TREE_BUILDING_INIT_START_TIME));
        accumulate(init_Finish_Time, accums.get(QfragRunner.TREE_BUILDING_INIT_FINISH_TIME));
        accumulate(computation_Start_Time, accums.get(QfragRunner.TREE_BUILDING_COMPUTATION_START_TIME));
        accumulate(computation_Finish_Time, accums.get(QfragRunner.TREE_BUILDING_COMPUTATION_FINISH_TIME));
    }
}
