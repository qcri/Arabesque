package io.arabesque.search.steps;

import io.arabesque.conf.Configuration;
import io.arabesque.graph.BasicMainGraphQuery;
import io.arabesque.graph.SearchGraph;
import io.arabesque.search.trees.SearchDataTree;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Created by mserafini on 1/27/16.
 */
public class QueryGraph implements Externalizable {
    private static final Logger LOG = Logger.getLogger(QueryGraph.class);
    static int STAR_LABEL = -1;
    private static final String QUERY_FILE_NAME = "arabesque.search.querygraph";
    private static final String START_FULL      = "arabesque.search.full_start";

    // ######################### INPUT GRAPHS #########################
    private boolean has_star_label_on_edge;
    private BasicMainGraphQuery queryGraph;
    private boolean              isGraphEdgeLabelled;
//    private boolean isGraphMulti;

    // ######################### ROOT SELECTION #########################

    private Node root;

    // ######################### DFS ORDERING VARIABLES - updated by advanceDFS #########################

    private IntArrayList childToFatherDFS;
    private ArrayList<IntArrayList> fatherToChildrenDFS;
    private ArrayList<Node> DFSPosToNode;
    private Int2IntOpenHashMap vertexIdToDFSPos;
//    private IntArrayList DFSPosInBFSOrder = new IntArrayList();

    // ######################### CROSS EDGE INDEXES  #########################

    // map: node in the tree -> list of nodes with cross edges
    // updated by the constructor when it does the spanning tree
    private HashMap<Node, ArrayList<Node>> crossEdges;
    // the following two structures are used to avoid creating iterators to scan crossEdges
    // they are built when crossEdges are not changed anymore
    private ArrayList<Node> crossEdgesKeys;
    private ArrayList<ArrayList<Node>> crossEdgesValues;

    // the following are computed by buildCrossEdgeIndex using crossEdges
    // position i (in DFS order) -> list of positions in DFS order to which there must be edges
    private ArrayList<IntArrayList> crossEdgesDFSIndex;
    // position i (in DFS order) -> index of crossEdgesDFSIndex -> list of edge labels for the cross edge (for multi-graphs)
    private ArrayList<ArrayList<IntArrayList>> crossEdgeDFSIndexLabels;

    // position i (in DFS order) -> list of positions in DFS order whose vertex id must be smaller
    private ArrayList<IntArrayList> smallerIdDFSIndex;

    // ######################### MATCHING ORDER #########################

    // updated by the constructor in BFS visit, read to compute matching orders
    private ArrayList<Node> leafNodes = new ArrayList<>();
    private IntOpenHashSet leafDFSPos = new IntOpenHashSet();

    // ###################################################################

    public QueryGraph() {};

    public QueryGraph(String queryFileName) {

        LOG.info("QFrag: Building query tree");

        long start = System.currentTimeMillis();

        // obtain input data
        Configuration conf = Configuration.get();

        if (queryFileName == null) {
            throw new RuntimeException("Name of the query file missing from yaml file");
        }

        isGraphEdgeLabelled = conf.isGraphEdgeLabelled();

        boolean full_start = conf.getBoolean(START_FULL,true);

        Path queryFile = new Path(queryFileName);

        try {
            queryGraph = new BasicMainGraphQuery(queryFile);
        } catch (IOException e) {
            throw new RuntimeException("Problem reading query file " + queryFileName + ": " + e.toString());
        }
        has_star_label_on_edge = queryGraph.has_star_on_edges();
        //LOG.info("QFrag: Query graph has size " + queryGraph.getNumberVertices());

        int numVertices = queryGraph.getNumberVertices();
        int numEdges = queryGraph.getNumberEdges();

        //queryGraph.checkMe();

        int maxSizeEmbedding = Math.max(numVertices, numEdges);

        fatherToChildrenDFS = new ArrayList<>();
        for (int i = 0; i < numVertices; i++){
            fatherToChildrenDFS.add(new IntArrayList());
        }

        crossEdges = new HashMap<>(maxSizeEmbedding);

        // determine root vertex and its matches

        int rootVertexId = findRootVertexAndMatches(full_start);

        //LOG.info("QFrag: Query start vertex: " + rootVertexId);

        // create root node in the query tree and init DFS search
        root = new Node(rootVertexId, null, -1, numVertices);

        // build rest of the tree using BFS (gets minimal root-leaf distance)

        HashIntSet visitedQueryVertices = HashIntSets.newMutableSet(numVertices);
        visitedQueryVertices.add(rootVertexId);

        HashMap<Integer, Node> queryVertexIdToNode = new HashMap<>();
        queryVertexIdToNode.put(rootVertexId, root);

        ArrayList<Node> currentNodes = new ArrayList<>(numVertices);
        ArrayList<Node> nextNodes = new ArrayList<>(numVertices);

        currentNodes.add(root);

        //LOG.info("QFrag: building the DFS tree");

        while (currentNodes.size() > 0) {

            //LOG.info("QFrag: CurrNodes:"+currentNodes);

            for (Node node : currentNodes) {
                int currVertexId = node.queryVertexId;

                //LOG.info("\tQFrag: vertex " + currVertexId);

                IntCollection neighborVertices =
                    queryGraph.getVertexNeighbours(currVertexId);

                node.degree = neighborVertices.size();

                boolean nodeIsLeaf = true;

                for (int destinationVertexId : neighborVertices) {

                    //LOG.info("\t\tQFrag: neighbor " + destinationVertexId);

                    if (node.parent != null && destinationVertexId == node.parent.queryVertexId) {
                        continue;
                    }

                    boolean isNewVertex = visitedQueryVertices.add(destinationVertexId);

                    if (isNewVertex) {

                        nodeIsLeaf = false;

                        //LOG.info("\t\t\tQFrag: is new vertex");

                        int nextSiblingId = node.children.size() + 1; // could use a simple counter here

                        Node newNode = new Node(destinationVertexId, node, nextSiblingId, numVertices);
                        node.children.add(newNode);

                        queryVertexIdToNode.put(destinationVertexId, newNode);

                        nextNodes.add(newNode);
                    } else {
                        //LOG.info("\t\t\tQFrag: is not new vertex");

                        Node otherNode = queryVertexIdToNode.get(destinationVertexId);
                        addCrossEdge(node, otherNode);
                        node.crossEdgesNum++;
                        otherNode.crossEdgesNum++;
                    }
                }

                if (nodeIsLeaf) {
                    leafNodes.add(node);
                }
            }

            ArrayList<Node> tmp = currentNodes;
            currentNodes = nextNodes;
            tmp.clear();
            nextNodes = tmp;
        }

        visitDFS();
//        computeDFSPosInBFSOrder();
        LOG.info("QFrag: Time for query tree and candidates: " + (System.currentTimeMillis()-start));
    }

    boolean has_star_label_on_edge() {
        return has_star_label_on_edge;
    }

    // used in constructor - careful!
    private int findRootVertexAndMatches(boolean full_start) {
        //long startTime = System.currentTimeMillis();

        IntArrayList rootMatchingVertices = new IntArrayList();
        int k = 3;

        //SearchGraph dataGraph = (SearchGraph) Configuration.get().getMainGraph();
        SearchGraph dataGraph = (SearchGraph) Configuration.get().getSearchMainGraph();

        // select top-k query vertices with highest rank
        int numQueryVertices = queryGraph.getNumberVertices();
        double[] topRanks = new double[numQueryVertices];
        int[] topVertices = new int[numQueryVertices];

        double currRank;
        int currPos = 0;
        double maxRank = Double.MIN_VALUE;
        int posMaxRank = -1;
        boolean allQueryVerticesMatched = true;
        for (int i = 0; i < numQueryVertices; i++) {
            int currQueryVertexLabel = queryGraph.getVertexLabel(i);

            if(currQueryVertexLabel == STAR_LABEL){
                continue;
            }

            currRank =
                    (double) dataGraph.getNumberVerticesWithLabel(
                        currQueryVertexLabel)
                    / (double) queryGraph.neighborhoodSize(i);

            if(currRank == 0){
                allQueryVerticesMatched = false;
                break;
            }

            if (currPos < k) {
                topRanks[currPos] = currRank;
                topVertices[currPos] = i;

                if (currPos == 0 || currRank > maxRank) {
                    maxRank = currRank;
                    posMaxRank = currPos;
                }
                currPos ++;
            } else {
                if (currRank < maxRank) {
                    topRanks[posMaxRank] = currRank;
                    topVertices[posMaxRank] = i;

                    for (int j = 0; j < k; j++) {
                        if (topRanks[j] < currRank) {
                            maxRank = topRanks[j];
                            posMaxRank = j;
                        }
                    }
                }
            }
        }

        if (!allQueryVerticesMatched){
            throw new RuntimeException("Query has no matches - TODO: implement proper clean exit");
        }

        if(currPos == 0){
            // all stars: return the query vertex with highest degree.

            int maxNeighborhoodSize = -1;
            int startQueryVertex = -1;
            for(int i = 0; i < numQueryVertices; i++) {
                if (queryGraph.neighborhoodSize(i) > maxNeighborhoodSize){
                    maxNeighborhoodSize = queryGraph.neighborhoodSize(i);
                    startQueryVertex = i;
                }
            }

            int numDataVertices = dataGraph.getNumberVertices();
            for (int i = 0; i < numDataVertices; i++){
                if (dataGraph.neighborhoodSize(i) >= maxNeighborhoodSize){
                    rootMatchingVertices.add(i);
                }
            }
            // if the above is too expensive, can simply do
            // rootMatchingVertices = null;

            return startQueryVertex;

        } else {
            k = currPos;
        }

            // select one start query vertex among the top-k by using
        // more expensive filters
        int minNumberRegions = Integer.MAX_VALUE;
        IntArrayList currStartCandidates = new IntArrayList();
        int startQueryVertexId = 0;

        // queryVertex -> label -> neighbors with label
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> neighborhoodSizePerLabel = new Int2ObjectOpenHashMap<>();
        for (int queryVertex : topVertices){
            Int2IntOpenHashMap labelCardinality = new Int2IntOpenHashMap();
            neighborhoodSizePerLabel.put(queryVertex, labelCardinality);
            for (int label : queryGraph.getAllLabelsNeighbors(queryVertex)){
                int queryNeighborsWithLabel =
                        queryGraph.getNeighborhoodSizeWithLabel(queryVertex, label);
                labelCardinality.put(label,queryNeighborsWithLabel);
            }
        }

        // find query vertex with minimum rank and place it in position 0 of topVertices
        // its candidates are filtered first and other query vertices are considered only if they are selective
        int totalDataVertices = dataGraph.getNumberVertices();
        double minRank = Double.MAX_VALUE;
        int minPos = -1;
        for (int i = 0; i < k; i++){
            if (topRanks[i] < minRank){
                minRank = topRanks[i];
                minPos = i;
            }
        }
        int tmp = topVertices[minPos];
        topVertices[minPos] = topVertices[0];
        topVertices[0] = tmp;

        // filter candidate regions for each candidate query vertex
        for (int i = 0; i < k; i ++){
            int currQueryVertex = topVertices[i];

            if (!rootMatchingVertices.isEmpty() && !full_start){
                continue;
            }

            IntArrayList candidates = dataGraph.getVerticesWithLabel(
                queryGraph.getVertexLabel(currQueryVertex));

            if (candidates == null) {
//                LOG.info("no vertex with label " +
//                        queryGraph.getVertexLabel(currQueryVertex) + " exists");
                //System.exit(0);
                continue;
            }

            // skip selection for non-selective query vertices unless we have no candidates yet
            if (!rootMatchingVertices.isEmpty() && candidates.size() / totalDataVertices > 0.05){
                continue;
            }

            currStartCandidates.clear();

            int currNumberRegions = 0;
//            int numDataVertices = dataGraph.getNumberVertices();

            //LOG.info("QFrag: Candidates for query vertex " + currQueryVertex);
            //LOG.info("QFrag: candidates:"+candidates);

            int queryVertexNeighborhoodSize
                    = queryGraph.neighborhoodSize(currQueryVertex);

            for (int j = 0; j< candidates.size(); j++){
                int candidate = candidates.getUnchecked(j);
                int candidateNeighborhoodSize
                    = dataGraph.neighborhoodSize(candidate);

//                if (candidateNeighborhoodSize / numDataVertices < 0.05){
//                    LOG.info("Candidate has too little support");
//                    continue;
//                }

                //LOG.info("Data Candidate:"+candidate+"  neigh size:"+
                //    candidateNeighborhoodSize+" target:"+queryVertexNeighborhoodSize);
                if (candidateNeighborhoodSize < queryVertexNeighborhoodSize) {
                    //LOG.info("QFrag: Candidate has too few neighbors");
                    continue;
                }

                boolean NLFPilterPass = true;

                Int2IntOpenHashMap labelCardinality = neighborhoodSizePerLabel.get(currQueryVertex);
                for (Int2IntMap.Entry entry : labelCardinality.int2IntEntrySet()){
                    int label = entry.getKey();
                    int queryNeighborsWithLabel = entry.getValue();

                    int candidateNeighborsWithLabel =
                            dataGraph.getNeighborhoodSizeWithLabel(candidate, label);

                    if(candidateNeighborsWithLabel < queryNeighborsWithLabel){
                        NLFPilterPass = false;
                        break;
                    }
                }


                if (!NLFPilterPass) {
                    continue;
                }

                //LOG.info("QFrag: Adding candidate " + candidate);

                currNumberRegions++;
                currStartCandidates.add(candidate);
            }

            if (currNumberRegions < minNumberRegions) {
                minNumberRegions = currNumberRegions;
                rootMatchingVertices.clear();
                rootMatchingVertices.addAll(currStartCandidates);
                startQueryVertexId = currQueryVertex;
            }
        }

        if(rootMatchingVertices.isEmpty()){
            throw new RuntimeException("Query has no matches - TODO: implement proper clean exit");
        }
        return startQueryVertexId;
    }

    // ###### NOTE ###### used in constructor - careful!
    private void visitDFS(){

        Node DFSCurrNode = root;
        boolean DFSVisitCompleted = false;
        int DFSNextVertexPos = 0;

        DFSPosToNode = new ArrayList<>();
        childToFatherDFS = new IntArrayList();
        vertexIdToDFSPos = new Int2IntOpenHashMap();

        //LOG.info("QFrag: Advancing DFS ");


        while(!DFSVisitCompleted) {

            // ##### record order of current node #####

            LOG.info("QFrag: DFS order for query node with vertex id " + DFSCurrNode.queryVertexId
                    + " and label " + queryGraph.getVertexLabel(DFSCurrNode.queryVertexId)
                    + " is " + DFSNextVertexPos);
            if (DFSCurrNode.parent != null){
                LOG.info("QFrag: Parent query vertex id is " + DFSCurrNode.parent.queryVertexId);
            }

            DFSCurrNode.DFSOrder = DFSNextVertexPos;
            vertexIdToDFSPos.put(DFSCurrNode.queryVertexId, DFSNextVertexPos);
            DFSNextVertexPos++;

            if (DFSCurrNode.parent == null){
                // node is root
                childToFatherDFS.add(-1);
            } else {
                childToFatherDFS.add(DFSCurrNode.parent.DFSOrder);
                fatherToChildrenDFS.get(DFSCurrNode.parent.DFSOrder).add(DFSCurrNode.DFSOrder);
            }

            DFSPosToNode.add(DFSCurrNode);

//        LOG.info("QFrag: DFS edge position is " + node.DFSLastEdgeFromParent);

            // ##### advance DFS search #####

            if (!DFSCurrNode.children.isEmpty()) {
                DFSCurrNode = DFSCurrNode.children.get(0);
                //            LOG.info("QFrag: moving to child. Current query vertex: " + child.queryVertexId);
            } else {

                leafDFSPos.add(DFSCurrNode.DFSOrder);

                while (DFSCurrNode != root && DFSCurrNode.nextSiblingId == DFSCurrNode.parent.children.size()) {
                    // move upwards until find parent with unvisited siblings (or reach root)
                    DFSCurrNode = DFSCurrNode.parent;
                    //                LOG.info("QFrag: moving to parent. Current query vertex: " + parent.queryVertexId);
                }

                if (DFSCurrNode == root) {
                    // reached root: no node with unvisited siblings: finished DFS search
                    DFSVisitCompleted = true;
                } else {

                    int nextSiblingId = DFSCurrNode.nextSiblingId;
                    DFSCurrNode = DFSCurrNode.parent.children.get(nextSiblingId);
                    //                LOG.info("QFrag: moving to sibling. Current query vertex: " + sibling.queryVertexId);
                }
            }
        }

        //LOG.info("QFrag: Terminating DFS ");

        // ##### terminate DFS search #####

        computeMatchingOrderReductionFactor(root);

        buildCrossEdgeIndex();
        buildSmallerIdIndex();

    }

    private void addCrossEdge(Node sourceNode, Node destinationNode) {
        //LOG.info("QFrag: Adding cross edge to node: " + sourceNode.queryVertexId
        //    + " edge is " + sourceNode.queryVertexId + "-" +
        //    destinationNode.queryVertexId);
        ArrayList<Node> edges = crossEdges.get(sourceNode);
        if (edges == null) {
            edges = new ArrayList<>();
            crossEdges.put(sourceNode, edges);
        }
        edges.add(destinationNode);
    }

    public boolean hasCrossEdges(){
        return !crossEdges.isEmpty();
    }

    private void buildCrossEdgeIndex() {
        int numVertices = queryGraph.getNumberVertices();
        int numEdges = queryGraph.getNumberEdges();

        int maxSizeEmbedding = Math.max(numVertices, numEdges);

        crossEdgesDFSIndex = new ArrayList<>(maxSizeEmbedding);
        crossEdgeDFSIndexLabels = new ArrayList<>(maxSizeEmbedding);

        for (int i = 0; i < maxSizeEmbedding; i++) {
            crossEdgesDFSIndex.add(new IntArrayList(maxSizeEmbedding));
            crossEdgeDFSIndexLabels.add(
                new ArrayList<IntArrayList>(maxSizeEmbedding));
        }

        //LOG.info("QFrag: Building cross edge array! (vertex-based)");

        //if (crossEdges == null) {
            //LOG.info("QFrag: crossEdges is null!");
        //}

        crossEdgesKeys = new ArrayList<>();
        crossEdgesValues = new ArrayList<>();
        for (Map.Entry<Node, ArrayList<Node>> entry : crossEdges.entrySet()) {
            crossEdgesKeys.add(entry.getKey());
            crossEdgesValues.add(entry.getValue());
        }

        for (int i = 0; i < crossEdges.size(); i++) {
            int sourceDFSPos;
            sourceDFSPos = crossEdgesKeys.get(i).DFSOrder;
            //LOG.info("QFrag: Considering source " + entry.getKey().queryVertexId + " with DFS position " + sourceDFSPos);

            ArrayList<Node> crossEdgeList = crossEdgesValues.get(i);
            for (Node destination : crossEdgeList) {

                int destDFSPos = destination.DFSOrder;

                //LOG.info("QFrag: Considering destination " + destination.queryVertexId + " with DFS position " + destDFSPos);
                crossEdgesDFSIndex.get(sourceDFSPos).add(destDFSPos);
                if (isGraphEdgeLabelled){
                    ReclaimableIntCollection edgeIds = queryGraph.getEdgeIds(crossEdgesKeys.get(i).queryVertexId,
                            destination.queryVertexId);
                    crossEdgeDFSIndexLabels.get(sourceDFSPos).add(getEdgeLabelsToParent(edgeIds));
                }
            }
        }

        // TODO free up the data structures
//            crossEdges = null;
//            queryVertexToDFSPos = null;
    }

    private void buildSmallerIdIndex(){
        smallerIdDFSIndex = new ArrayList<>(DFSPosToNode.size());
        for (Node queryVertex: DFSPosToNode){
            IntArrayList smallerIdsDFS = new IntArrayList();
            smallerIdDFSIndex.add(smallerIdsDFS);

            IntArrayList smallerIdArray = queryGraph.getSmallerIds(queryVertex.queryVertexId);
            if (smallerIdArray != null) {
                for (int i = 0; i < smallerIdArray.size(); i++) {
                    int smallerVertexId = smallerIdArray.get(i);
                    smallerIdsDFS.add(vertexIdToDFSPos.get(smallerVertexId));
                }
            }
//            LOG.info("QFrag: smallerIdDFSIndex: " + smallerIdsDFS);
        }
    }


    private IntArrayList getEdgeLabelsToParent (ReclaimableIntCollection edgeIDsToParent) {
        if(!isGraphEdgeLabelled || edgeIDsToParent == null) {
            return null;
        }
        else {
            IntArrayList edgeLabelsToParent = new IntArrayList();
            for (int edgeId : edgeIDsToParent) {
                edgeLabelsToParent.add(queryGraph.getEdgeLabel(edgeId));
            }
            return edgeLabelsToParent;
        }
    }

    public IntArrayList getMatchingOrder(SearchDataTree dataTree, IntArrayList matchingOrder) {
        matchingOrder.clear();

        double[] leafWeights = new double[leafNodes.size()];

        for (int leafNodesPos = 0; leafNodesPos < leafNodes.size(); leafNodesPos++) {
            Node node = leafNodes.get(leafNodesPos);

            int cardinality = dataTree.getCardinality(node.DFSOrder);
            leafWeights[leafNodesPos] = cardinality * node.matchingOrderReductionFactor;
        }

        int sortedLeafs = 0;

        while (sortedLeafs < leafNodes.size()) {

            double minWeight = Double.MAX_VALUE;
            int minPos = -1;
            for (int i = 0; i < leafWeights.length; i++) {
                double weight = leafWeights[i];
                if (weight < minWeight) {
                    minWeight = weight;
                    minPos = i;
                }
            }

            if (minWeight == Double.MAX_VALUE) {
               // LOG.error("Something went wrong when sorting path leafs. Exiting");
                System.exit(1);
            }

            // make sure we don't pick this node again
            leafWeights[minPos] = Double.MAX_VALUE;

            Node currNode = leafNodes.get(minPos);

            if(currNode == null){
                throw new RuntimeException("Marco - currNode is null, minPos is " + minPos
                        + " and leafNodes has size " + leafNodes.size());
            }

            if(currNode.path == null){
                throw new RuntimeException("Marco - currNode.path is null, minPos is " + minPos
                        + " and leafNodes has size " + leafNodes.size() + " currNode has id "
                        + currNode.queryVertexId + " and its parent is " + currNode.parent.queryVertexId);
            }

            for (int i = 0; i < currNode.path.size(); i++){
                Node pathNode = currNode.path.get(i);
                int DFSPos = pathNode.DFSOrder;
                if (!matchingOrder.contains(DFSPos)) {
                    pathNode.matchingOrder = matchingOrder.size();
                    matchingOrder.add(DFSPos);
                }
            }

            sortedLeafs++;
        }

        return matchingOrder;
    }

    private void computeMatchingOrderReductionFactor(Node node){

        if (node != root) {
            node.matchingOrderReductionFactor =
                node.parent.matchingOrderReductionFactor / (node.crossEdgesNum + 1);
        }

        for (Node child : node.children) {
            computeMatchingOrderReductionFactor(child);
        }
    }

    public IntArrayList getCrossEdgesDestVertex(int sourcePosition){
        return crossEdgesDFSIndex.get(sourcePosition);
    }

    ArrayList<IntArrayList> getLabelsCrossEdgesDestVertex(int sourcePosition){
        return crossEdgeDFSIndexLabels.get(sourcePosition);
    }

    public boolean isLeaf(int DFSpos){
        return leafDFSPos.contains(DFSpos);
    }

    public int getChildToFatherDFS(int childDFSPos) {
        return childToFatherDFS.getUnchecked(childDFSPos);
    }

    public IntArrayList getFatherToChildrenDFS(int fatherDFSPos){
        return fatherToChildrenDFS.get(fatherDFSPos);
    }

//    private void computeDFSPosInBFSOrder(){
//
//        //TODO check and use this order to scan the domains in the split
//
//        int arraySize = queryGraph.getNumberVertices();
//
//        DFSPosInBFSOrder = new IntArrayList(arraySize);
//        ArrayList<Node> currDepth = new ArrayList<>(arraySize);
//        ArrayList<Node> nextDepth = new ArrayList<>(arraySize);
//
//        currDepth.add(root);
//        while(!currDepth.isEmpty()) {
//            for (Node node : currDepth) {
//                DFSPosInBFSOrder.add(node.DFSOrder);
//                nextDepth.addAll(node.children);
//            }
//            currDepth = nextDepth;
//            nextDepth = new ArrayList<>(arraySize);
//        }
//    }

//    public int getDFSPosAtBFSOrder (int BFSPos){
//        return DFSPosInBFSOrder.getUnchecked(BFSPos);
//    }

    IntArrayList getRootMatchingVertices(){
        int rootLabel = queryGraph.getVertexLabel(root.queryVertexId);
        //SearchGraph dataGraph = (SearchGraph) Configuration.get().getMainGraph();
        SearchGraph dataGraph = (SearchGraph) Configuration.get().getSearchMainGraph();
        return dataGraph.getVerticesWithLabel(rootLabel);
    }

    public int getNumberVertices() {
        return queryGraph.getNumberVertices();
    }

    public IntArrayList getSmallerIdDFSIndex(int vertexDFSPos){
        return smallerIdDFSIndex.get(vertexDFSPos);
    }

    int getDestinationLabel(int destDFSPos){
        Node destNode = DFSPosToNode.get(destDFSPos);
        return queryGraph.getVertexLabel(destNode.queryVertexId);
    }

    int getDestinationDegree(int destDFSPos){
        Node destNode = DFSPosToNode.get(destDFSPos);
        return destNode.degree;
    }

    IntArrayList getEdgeLabels(int destDFSPos){
        if (!isGraphEdgeLabelled ) {
            return null;
        }

        Node destNode = DFSPosToNode.get(destDFSPos);
        ReclaimableIntCollection edgeIds = queryGraph.getEdgeIds(destNode.parent.queryVertexId,
                destNode.queryVertexId);
        return getEdgeLabelsToParent(edgeIds);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (crossEdges != null) {
            for (Map.Entry<Node, ArrayList<Node>> entry : crossEdges.entrySet()) {
                str.append("### ").append(entry.getKey().queryVertexId).append(" = ");
                for (Node n : entry.getValue()) {
                    str.append(n.queryVertexId).append(",");
                }
            }
        }
        return str.toString();
    }

    public long getMatchingCostPerEmbedding(SearchDataTree dataTree){
        long cost = 0;

        for (int j = 0; j < crossEdges.size(); j++){

            Node source = crossEdgesKeys.get(j);
            int sourceQueryVertexId = source.queryVertexId;
            long costSource = 1;

            ArrayList<Node> destinations = crossEdgesValues.get(j);
            for (int i = 0; i < destinations.size(); i++){

                Node destination = destinations.get(i);
                int destinationQueryVertexId = destination.queryVertexId;
                // this check is to avoid double counting
                if(destinationQueryVertexId > sourceQueryVertexId){
                    int destDFSPos = destination.DFSOrder;
                    long destCardinality = dataTree.getCardinality(destDFSPos);
                    costSource *= destCardinality;
                }
            }
            cost += costSource;
        }
        return cost;
    }

    public int getDFSPosSplitDomain(SearchDataTree dataTree){
        if(crossEdges.isEmpty()){
            return -1;
        }

        getMatchingOrder(dataTree, new IntArrayList());

        int minMatchingOrder = Integer.MAX_VALUE;
        int DFSPos = -1;
        for(Node node : leafNodes){
            // if the leaf has no cross edges, go to parent
            while(node.crossEdgesNum == 0 && node.parent != null){
                node = node.parent;
            }
            if(node.crossEdgesNum != 0
                    && node.matchingOrder != -1
                    && node.matchingOrder < minMatchingOrder){
                minMatchingOrder = node.matchingOrder;
                DFSPos = node.DFSOrder;
            }
        }

        if(DFSPos == -1){
            // no cross edges, take the child of the root with highest cardinality
            //TODO consider cardinality of the whole subtree
            ArrayList<Node> children = root.children;
            int maxCardinality = -1;
            for (int i = 0; i < children.size(); i++){
                Node child = children.get(i);
                int DFSPosChild = child.DFSOrder;
                int childCardinality = dataTree.getCardinality(DFSPosChild);
                if (childCardinality > maxCardinality){
                    maxCardinality = childCardinality;
                    DFSPos = DFSPosChild;
                }
            }
        }
        return DFSPos;
    }

    public long getNumberEmbeddingsMatched(SearchDataTree dataTree, long numberEmbeddingsExplored){
        for (int i = 0; i < leafNodes.size(); i++){
            Node leafNode = leafNodes.get(i);
            numberEmbeddingsExplored *= dataTree.getCardinality(leafNode.DFSOrder);
        }
        return numberEmbeddingsExplored;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(STAR_LABEL);
        out.writeBoolean(has_star_label_on_edge);
        out.writeBoolean(isGraphEdgeLabelled);

//        //### rootMatchingVertices
//        if (rootMatchingVertices == null){
//            out.writeInt(-1);
//        } else {
//            int size = rootMatchingVertices.size();
//            out.writeInt(size);
//            for (int i=0; i < size; i++){
//                out.writeInt(rootMatchingVertices.get(i));
//            }
//        }

        //### root
        out.writeInt(queryGraph.getNumberVertices());
        root.writeSubTree(out);

        //### DFS ordering variables

        if (childToFatherDFS == null){
            out.writeInt(-1);
        } else {
            int size = childToFatherDFS.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                out.writeInt(childToFatherDFS.get(i));
            }
        }

        if (fatherToChildrenDFS == null){
            out.writeInt(-1);
        } else {
            int size = fatherToChildrenDFS.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                IntArrayList list = fatherToChildrenDFS.get(i);
                if (list == null) {
                    out.writeInt(-1);
                } else {
                    int size2 = list.size();
                    out.writeInt(size2);
                    for (int j=0; j < size2; j++){
                        out.writeInt(list.get(j));
                    }
                }
            }
        }

        if (DFSPosToNode == null){
            out.writeInt(-1);
        } else {
            int size = DFSPosToNode.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                Node node = DFSPosToNode.get(i);
                if (node == null){
                    out.writeInt(-1);
                } else {
                    // to be converted to pointer when deserialized
                    out.writeInt(node.queryVertexId);
                }
            }
        }

        if(vertexIdToDFSPos == null){
            out.writeInt(-1);
        } else {
            ObjectSet<Map.Entry<Integer,Integer>> entries = vertexIdToDFSPos.entrySet();
            out.writeInt(entries.size());
            for (Map.Entry<Integer,Integer> entry : entries){
                out.writeInt(entry.getKey());
                out.writeInt(entry.getValue());
            }
        }

        //### cross edge indices

        if (crossEdges == null){
            out.writeInt(-1);
        } else {
            Set<Map.Entry<Node, ArrayList<Node>>> entries = crossEdges.entrySet();
            out.writeInt(entries.size());
            for (Map.Entry<Node, ArrayList<Node>> entry : entries){
                Node node = entry.getKey();
                if (node == null){
                    out.writeInt(-1);
                } else {
                    // to be converted to pointer when deserialized
                    out.writeInt(node.queryVertexId);
                }
                ArrayList<Node> list = entry.getValue();
                if (list == null){
                    out.writeInt(-1);
                } else {
                    out.writeInt(list.size());
                    for (Node node2 : list){
                        if (node2 == null){
                            out.writeInt(-1);
                        } else {
                            // to be converted to pointer when deserialized
                            out.writeInt(node2.queryVertexId);
                        }
                    }
                }
            }

        }

        if (crossEdgesKeys == null){
            out.writeInt(-1);
        } else {
            int size = crossEdgesKeys.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                Node node = crossEdgesKeys.get(i);
                if (node == null){
                    out.writeInt(-1);
                } else {
                    // to be converted to pointer when deserialized
                    out.writeInt(node.queryVertexId);
                }
            }
        }

        if (crossEdgesValues == null){
            out.writeInt(-1);
        } else {
            int size = crossEdgesValues.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                ArrayList<Node> list = crossEdgesValues.get(i);
                if (list == null){
                    out.writeInt(-1);
                } else {
                    int size2 = list.size();
                    out.writeInt(size2);
                    for (int j=0; j < size2; j++){
                        Node node = list.get(j);
                        if (node == null){
                            out.writeInt(-1);
                        } else {
                            // to be converted to pointer when deserialized
                            out.writeInt(node.queryVertexId);
                        }
                    }
                }
            }
        }

        if(crossEdgesDFSIndex == null){
            out.writeInt(-1);
        } else {
            int size = crossEdgesDFSIndex.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                IntArrayList list = crossEdgesDFSIndex.get(i);
                if (list == null){
                    out.writeInt(-1);
                } else {
                    int size2 = list.size();
                    out.writeInt(size2);
                    for (int j=0; j < size2; j++){
                        out.writeInt(list.get(j));
                    }
                }
            }
        }

        if (crossEdgeDFSIndexLabels == null){
            out.writeInt(-1);
        } else {
            int size = crossEdgeDFSIndexLabels.size();
            out.writeInt(size);
            for (ArrayList<IntArrayList> list : crossEdgeDFSIndexLabels){
                if (list == null){
                    out.writeInt(-1);
                } else {
                    out.writeInt(list.size());
                    for (IntArrayList list2 : list){
                        if (list2 == null){
                            out.writeInt(-1);
                        } else {
                            int size2 = list2.size();
                            out.writeInt(size2);
                            for (int i=0; i < size2; i++){
                                out.writeInt(list2.get(i));
                            }
                        }
                    }
                }
            }
        }

        if(smallerIdDFSIndex == null){
            out.writeInt(-1);
        } else {
            int size = smallerIdDFSIndex.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                IntArrayList list = smallerIdDFSIndex.get(i);
                if (list == null){
                    out.writeInt(-1);
                } else {
                    int size2 = list.size();
                    out.writeInt(size2);
                    for (int j=0; j < size2; j++) {
                        out.writeInt(list.get(j));
                    }
                }
            }
        }

        //### matching order

        if (leafNodes == null){
            out.writeInt(-1);
        } else {
            int size = leafNodes.size();
            out.writeInt(size);
            for (int i=0; i < size; i++){
                Node node = leafNodes.get(i);
                if (node == null){
                    out.writeInt(-1);
                } else {
                    // to be converted to pointer when deserialized
                    out.writeInt(node.queryVertexId);
                }
            }
        }

        if (leafDFSPos == null){
            out.writeInt(-1);
        } else {
            int size = leafDFSPos.size();
            out.writeInt(size);
            IntIterator iter = leafDFSPos.iterator();
            while (iter.hasNext()){
                out.writeInt(iter.nextInt());
            }
        }


        //### queryGraph
        queryGraph.write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        STAR_LABEL = in.readInt();
        has_star_label_on_edge = in.readBoolean();
        isGraphEdgeLabelled = in.readBoolean();

//        //### rootMatchingVertices
//        rootMatchingVertices = null;
//        int size = in.readInt();
//        if (size >= 0){
//            rootMatchingVertices = new IntArrayList(size);
//            for (int i=0; i < size; i++) {
//                rootMatchingVertices.add(in.readInt());
//            }
//        }

        //### query spanning tree (root)
        HashMap<Integer, Node> readNodes = new HashMap<>();
        HashMap<Node, Integer> parentsMap = new HashMap<>();
        HashMap<Node, ArrayList<Integer>> childrenMap = new HashMap<>();
        HashMap<Node, ArrayList<Integer>> pathsMap = new HashMap<>();

        int numNodes = in.readInt();
        root = new Node();
        root.readNode(in, readNodes, parentsMap, childrenMap, pathsMap);
        for (int i=0; i < numNodes-1; i++){
            Node node = new Node();
            node.readNode(in, readNodes, parentsMap, childrenMap, pathsMap);
        }

        for (Map.Entry<Node, Integer> parentsEntry : parentsMap.entrySet()){
            Node child = parentsEntry.getKey();
            int parentId = parentsEntry.getValue();
            Node parentNode = readNodes.get(parentId);
            child.parent = parentNode;
        }

        for (Map.Entry<Node, ArrayList<Integer>> childrenEntry : childrenMap.entrySet()){
            Node parent = childrenEntry.getKey();
            ArrayList<Integer> childrenList = childrenEntry.getValue();
            parent.children = new ArrayList<>();
            for (int childId : childrenList){
                Node childNode = readNodes.get(childId);
                parent.children.add(childNode);
            }
        }

        for (Map.Entry<Node, ArrayList<Integer>> pathEntry :  pathsMap.entrySet()){
            Node parent = pathEntry.getKey();
            ArrayList<Integer> pathList = pathEntry.getValue();
            parent.path = new ArrayList<>();
            for (int pathId : pathList){
                Node pathNode = readNodes.get(pathId);
                parent.path.add(pathNode);
            }
        }

        //### DFS ordering variables

        childToFatherDFS = null;
        int size = in.readInt();
        if (size >= 0){
            childToFatherDFS = new IntArrayList(size);
            for (int i=0; i < size; i++){
                childToFatherDFS.add(in.readInt());
            }
        }

        fatherToChildrenDFS = null;
        size = in.readInt();
        if (size >= 0){
            fatherToChildrenDFS = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                IntArrayList list = null;
                int listSize = in.readInt();
                if (listSize >= 0){
                    list = new IntArrayList(listSize);
                    for (int j=0; j < listSize; j++){
                        list.add(in.readInt());
                    }
                }
                fatherToChildrenDFS.add(list);
            }
        }

        DFSPosToNode = null;
        size = in.readInt();
        if (size >= 0){
            DFSPosToNode = new ArrayList<> (size);
            for (int i=0; i < size; i++){
                Node node = null;
                int queryVertexId = in.readInt();
                if (queryVertexId >= 0) {
                    node = readNodes.get(queryVertexId);
                }
                DFSPosToNode.add(node);
            }
        }

        vertexIdToDFSPos = null;
        size = in.readInt();
        if (size >= 0){
            vertexIdToDFSPos = new Int2IntOpenHashMap(size);
            for (int i=0; i < size; i++){
                int key = in.readInt();
                int value = in.readInt();
                vertexIdToDFSPos.put(key,value);
            }
        }

        //### cross edge indices

        crossEdges = null;
        size = in.readInt();
        if (size >= 0){
            crossEdges = new HashMap<>(size);
            for (int i=0; i < size; i++){
                Node key = null;
                int keyQueryVertexId = in.readInt();
                if (keyQueryVertexId >= 0){
                    key = readNodes.get(keyQueryVertexId);
                }
                ArrayList<Node> values = null;
                int valuesSize = in.readInt();
                if (valuesSize >= 0){
                    values = new ArrayList<>(valuesSize);
                    for (int j=0; j < valuesSize; j++){
                        Node value = null;
                        int valueQueryVertexId = in.readInt();
                        if (valueQueryVertexId >= 0){
                            value = readNodes.get(valueQueryVertexId);
                        }
                        values.add(value);
                    }
                }
                crossEdges.put(key,values);
            }
        }

        crossEdgesKeys = null;
        size = in.readInt();
        if (size >= 0){
            crossEdgesKeys = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                Node node = null;
                int nodeQueryVertexId = in.readInt();
                if (nodeQueryVertexId >= 0){
                    node = readNodes.get(nodeQueryVertexId);
                }
                crossEdgesKeys.add(node);
            }
        }

        crossEdgesValues = null;
        size = in.readInt();
        if (size >= 0){
            crossEdgesValues = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                ArrayList<Node> list = null;
                int listSize = in.readInt();
                if (listSize >= 0){
                    list = new ArrayList<>(listSize);
                    for (int j=0; j < listSize; j++){
                        Node node = null;
                        int nodeQueryVertexId = in.readInt();
                        if (nodeQueryVertexId >= 0){
                            node = readNodes.get(nodeQueryVertexId);
                        }
                        list.add(node);
                    }
                }
                crossEdgesValues.add(list);
            }
        }

        crossEdgesDFSIndex = null;
        size = in.readInt();
        if (size >= 0){
            crossEdgesDFSIndex = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                IntArrayList list = null;
                int listSize = in.readInt();
                if (listSize >= 0){
                    list = new IntArrayList(listSize);
                    for (int j=0; j < listSize; j++){
                        list.add(in.readInt());
                    }
                }
                crossEdgesDFSIndex.add(list);
            }
        }

        crossEdgeDFSIndexLabels = null;
        size = in.readInt();
        if (size >= 0){
            crossEdgeDFSIndexLabels = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                ArrayList<IntArrayList> list = null;
                int listSize = in.readInt();
                if (listSize >= 0){
                    list = new ArrayList<>(listSize);
                    for (int j=0; j < listSize; j++){
                        IntArrayList innerList = null;
                        int innerListSize = in.readInt();
                        if (innerListSize >= 0){
                            innerList = new IntArrayList(innerListSize);
                            for (int k=0; k < innerListSize; k++){
                                innerList.add(in.readInt());
                            }
                        }
                        list.add(innerList);
                    }
                }
                crossEdgeDFSIndexLabels.add(list);
            }
        }

        smallerIdDFSIndex = null;
        size = in.readInt();
        if (size >= 0){
            smallerIdDFSIndex = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                IntArrayList list = null;
                int listSize = in.readInt();
                if (listSize >= 0){
                    list = new IntArrayList(listSize);
                    for (int j=0; j < listSize; j++){
                        list.add(in.readInt());
                    }
                }
                smallerIdDFSIndex.add(list);
            }
        }

        //### matching order

        leafNodes = null;
        size = in.readInt();
        if (size >= 0){
            leafNodes = new ArrayList<>(size);
            for (int i=0; i < size; i++){
                Node node = null;
                int nodeQueryVertexId = in.readInt();
                if (nodeQueryVertexId >= 0){
                    node = readNodes.get(nodeQueryVertexId);
                }
                leafNodes.add(node);
            }
        }

        leafDFSPos = null;
        size = in.readInt();
        if (size >= 0){
            leafDFSPos = new IntOpenHashSet(size);
            for (int i=0; i < size; i++){
                leafDFSPos.add(in.readInt());
            }
        }

        //### queryGraph
        queryGraph = new BasicMainGraphQuery();
        queryGraph.read(in);
    }

    private static class Node {

        private int queryVertexId;

        private Node parent;
        private ArrayList<Node> children;

        private int nextSiblingId;

        private int DFSOrder = -1;
        private int matchingOrder = -1;

        private double matchingOrderReductionFactor = 1;
        private double crossEdgesNum = 0;

        private int degree;

        private ArrayList<Node> path;

        private Node () {}

        private Node(int queryVertexId, Node parent, int nextSiblingId, int maxChildren) {
            this.queryVertexId = queryVertexId;
            this.parent = parent;
            this.nextSiblingId = nextSiblingId;
            children  = new ArrayList<> (maxChildren);
            if(parent != null) {
                this.path = (ArrayList<Node>) parent.path.clone();
            } else {
                this.path = new ArrayList<Node>();
            }
            this.path.add(this);
        }


        public void writeSubTree(ObjectOutput out) throws IOException {

            out.writeInt(queryVertexId);
            out.writeInt(nextSiblingId);
            out.writeInt(DFSOrder);
            out.writeInt(matchingOrder);
            out.writeDouble(matchingOrderReductionFactor);
            out.writeDouble(crossEdgesNum);
            out.writeInt(degree);

            if (parent == null){
                out.writeInt(-1);
            } else {
                // the following must be translated to a pointer
                out.writeInt(parent.queryVertexId);
            }

            if (children == null){
                out.writeInt(-1);
            } else {
                int size = children.size();
                out.writeInt(size);
                for (int i=0; i < size; i++){
                    // the following must be translated to a pointer
                    out.writeInt(children.get(i).queryVertexId);
                }
            }

            if (path == null){
                out.writeInt(-1);
            } else {
                int size = path.size();
                out.writeInt(size);
                for (int i=0; i < size; i++){
                    // the following must be translated to a pointer
                    out.writeInt(path.get(i).queryVertexId);
                }
            }

            for (Node child:children){
                child.writeSubTree(out);
            }
        }

        private void readNode(ObjectInput in, HashMap<Integer, Node> readNodes, HashMap<Node, Integer> parentsMap,
                         HashMap<Node, ArrayList<Integer>> childrenMap, HashMap<Node, ArrayList<Integer>> pathsMap)
                throws IOException, ClassNotFoundException {

            queryVertexId = in.readInt();
            nextSiblingId = in.readInt();
            DFSOrder = in.readInt();
            matchingOrder = in.readInt();
            matchingOrderReductionFactor = in.readDouble();
            crossEdgesNum = in.readDouble();
            degree = in.readInt();

            readNodes.put(queryVertexId, this);

            int parentId = in.readInt();
            if (parentId < 0) {
                parent = null;
            } else {
                parentsMap.put(this, parentId);
            }

            int size = in.readInt();
            if (size < 0){
                children = null;
            } else {
                ArrayList<Integer> childrenArray = new ArrayList<>();
                childrenMap.put(this, childrenArray);
                for (int i=0; i < size; i++) {
                    int child = in.readInt();
                    childrenArray.add(child);
                }
            }

            size = in.readInt();
            if (size < 0){
                path = null;
            } else {
                ArrayList<Integer> pathArray = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    int nodeId = in.readInt();
                    pathArray.add(nodeId);
                }
                pathsMap.put(this,pathArray);
            }
        }
    }
}