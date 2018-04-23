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
//    private final double outlierPct = 0;
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

        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> configBC Before init = " + (configBC == null));
        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> configBC.value() Before init = " + (configBC.value() == null));
        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> queryGraphBC Before init = " + (queryGraphBC == null));
        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> queryGraphBC.value() Before init = " + (queryGraphBC.value() == null));

        configBC.value().initialize();
        Configuration conf = Configuration.get();

        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> configBC After init = " + (configBC == null));
        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> configBC.value() After init = " + (configBC.value() == null));
        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> queryGraphBC After init = " + (queryGraphBC == null));
        Log.info("@DEBUG_CONF In TreeBuilding.ctor() -> queryGraphBC.value() After init = " + (queryGraphBC.value() == null));

        String log_level = conf.getLogLevel();
        LOG.fatal("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));
/*        LogManager.getRootLogger().setLevel(Level.FATAL);
        Logger.getLogger("org").setLevel(Level.FATAL);
        Logger.getLogger("akka").setLevel(Level.FATAL);
        Logger.getLogger("spark").setLevel(Level.FATAL);
        Logger.getLogger("executor").setLevel(Level.FATAL);
        Logger.getLogger("memory").setLevel(Level.FATAL);
        Logger.getLogger("steps").setLevel(Level.FATAL);
        Logger.getLogger("storage").setLevel(Level.FATAL);
        Logger.getLogger("util").setLevel(Level.FATAL);*/

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
    public Iterator<Tuple2<Integer, SearchDataTree>> call(Integer partitionId,
                                                          Iterator<Tuple2<Integer, SearchDataTree>> v2) {

        // ###### Initialization of thread-local variables ######
//        LOG.info("I am partition " + partitionId + " running on thread " + Thread.currentThread().getName());

        //Log.info("@DEBUG_CONF In TreeBuilding.call() -> configBC.getMainGraph() before init = " + (configBC.value().getMainGraph() == null));
        System.out.println("@DEBUG_CONF In TreeBuilding.call() -> configBC.getMainGraph() before init = " + (configBC.value().getMainGraph() == null));

        Configuration conf = configBC.value();
        conf.initialize();
//        Log.info("@DEBUG_CONF In TreeBuilding.call() -> conf.getMainGraph() After init = " + (conf.getMainGraph() == null));
//        Log.info("@DEBUG_CONF In TreeBuilding.call() -> configBC.getMainGraph() After init = " + (configBC.value().getMainGraph() == null));
        System.out.println("@DEBUG_CONF In TreeBuilding.call() -> conf.getMainGraph() After init = " + (conf.getMainGraph() == null));
        System.out.println("@DEBUG_CONF In TreeBuilding.call() -> configBC.getMainGraph() After init = " + (configBC.value().getMainGraph() == null));

        // Modified from QFrag
        // UnsafeCSRGraphSearch dataGraph = Configuration.get().getMainGraph();
        //UnsafeCSRGraphSearch dataGraph = (UnsafeCSRGraphSearch)(Configuration.get().getMainGraph());
        UnsafeCSRGraphSearch dataGraph = (UnsafeCSRGraphSearch)(conf.getMainGraph());
        QueryGraph queryGraph = queryGraphBC.getValue();

//        Log.info("@DEBUG_CONF In TreeBuilding.call() -> queryGraphBC After init = " + (queryGraphBC == null));
//        Log.info("@DEBUG_CONF In TreeBuilding.call() -> queryGraphBC.value() After init = " + (queryGraphBC.value() == null));
        System.out.println("@DEBUG_CONF In TreeBuilding.call() -> queryGraphBC After init = " + (queryGraphBC == null));
        System.out.println("@DEBUG_CONF In TreeBuilding.call() -> queryGraphBC.value() After init = " + (queryGraphBC.value() == null));

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

    public class RuntimeAccumulator extends AccumulatorV2<Long, Long> {

        private long _value = 0;

        public RuntimeAccumulator() {
            this(0);
        }

        public RuntimeAccumulator(long initialValue) {
            if (initialValue != 0) {
                _value = initialValue;
            }
        }

        public void add(Long value) {
            _value = value() + value;
        }

        public RuntimeAccumulator copy() {
            return (new RuntimeAccumulator(value()));
        }

        public boolean isZero() {
            return (value() == 0);
        }

        public void merge(AccumulatorV2<Long, Long> other) {
            add(other.value());
        }

        public void reset() {
            _value = 0;
        }

        public Long value() {
            return _value;
        }
    }
}