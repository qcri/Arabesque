package io.arabesque.search.steps;

import io.arabesque.graph.SearchGraph;
import io.arabesque.search.trees.SearchDataTree;
import io.arabesque.search.trees.SearchEmbedding;
import io.arabesque.utils.ThreadOutput;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;

import static io.arabesque.search.steps.QueryGraph.STAR_LABEL;

class CrossEdgeMatchingLogic {
    private static final Logger LOG = Logger.getLogger(CrossEdgeMatchingLogic.class);

    private SearchGraph dataGraph;
    private QueryGraph queryGraph;
    private ThreadOutput outputStream;

    CrossEdgeMatchingLogic(SearchGraph dataGraph, QueryGraph queryGraph, ThreadOutput outputStream) {

        this.dataGraph = dataGraph;
        this.queryGraph = queryGraph;
        this.outputStream = outputStream;
    }

    void matchCrossEdges(SearchDataTree searchDataTree) {

        // if the data tree does not contain any embedding of the right size, skip
        if (!searchDataTree.hasFullEmbedding()){
            return;
        }

        searchDataTree.prepareScanMatchingOrder(queryGraph);
        IntArrayList matchingOrder = searchDataTree.getMatchingOrder();
        IntArrayList inverseMatchingOrder = searchDataTree.getInverseMatchingOrder();

        int queryVertices = queryGraph.getNumberVertices();

        SearchEmbedding embedding = searchDataTree.getNextEmbedding();

        while (embedding != null){

            if(!isCanonical(embedding, matchingOrder, inverseMatchingOrder) || !matchCrossEdgesLastVertex(embedding, matchingOrder, inverseMatchingOrder)){
                searchDataTree.pruneLastEmbeddingMatching();
                //TODO recursive pruning if we want to split the tree again after a few matches
            }
            else if(embedding.getSize() == queryVertices) {

                if (outputStream!=null) {
                    //If null output is disabled.
                    try {
                        outputStream.write(embedding);
                    } catch (IOException e) {
                        System.out.println("Could not write embeddings to Thread Output. Exiting.");
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
                //TODO recursive pruning from the searchDataTree if we want to split the tree again after a few matches
            }

            embedding = searchDataTree.getNextEmbedding();

        }
    }

    private boolean isCanonical(SearchEmbedding embedding, IntArrayList matchingOrder, IntArrayList inverseMatchingOrder){

        int lastVertexId = embedding.getLast();
        int lastVertexIdDFSPos = matchingOrder.get(embedding.getSize()-1);

        IntArrayList smallerIdsDFSPos = queryGraph.getSmallerIdDFSIndex(lastVertexIdDFSPos);
        if (smallerIdsDFSPos != null) {
            for (int i = 0; i < smallerIdsDFSPos.size(); i++) {
                int smallerIdDFSPos = smallerIdsDFSPos.get(i);
                int smallerIdMatchingPos = inverseMatchingOrder.get(smallerIdDFSPos);
                if (smallerIdMatchingPos < embedding.getSize()
                        && lastVertexId < embedding.getAtPos(smallerIdMatchingPos)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean matchCrossEdgesLastVertex(SearchEmbedding embedding, IntArrayList matchingOrder, IntArrayList inverseMatchingOrder) {

        // only match cross edges from the latest vertex added to vertices already added
        int sourceEmbeddingPos = embedding.getSize() - 1;
        int sourceDFSPos = matchingOrder.getUnchecked(sourceEmbeddingPos);
        int sourceVertexId = embedding.getLast();

        IntArrayList destArrayDFSPos = queryGraph.getCrossEdgesDestVertex(sourceDFSPos);
        ArrayList<IntArrayList> labelsArrayDFSPos = queryGraph.getLabelsCrossEdgesDestVertex(sourceDFSPos);

        if (destArrayDFSPos == null ) {
            return true;
        }

        //System.out.println("We have Cross edges:"+destArrayDFSPos);
        for (int i = 0; i < destArrayDFSPos.size(); i++) {

            int destDFSPos = destArrayDFSPos.getUnchecked(i);
            int destEmbeddingPos = inverseMatchingOrder.getUnchecked(destDFSPos);

            if (destEmbeddingPos >= embedding.getSize()) {
                // only match with destinations that are in the embedding already
                continue;
            }

            int destVertexId = embedding.getAtPos(destEmbeddingPos);

            //Check if neighbors with vertex label.
            if (!dataGraph.isNeighborVertexWithLabel(sourceVertexId, destVertexId,
                    queryGraph.getDestinationLabel(destDFSPos))) {
                return false;
            }

            if (dataGraph.isEdgeLabelled()) {
                IntArrayList edgeLabels = labelsArrayDFSPos.get(i);

                // First check if star label exists. If it does we are neighbors so don't check.
                if (!queryGraph.has_star_label_on_edge() || !edgeLabels.contains(STAR_LABEL)) {
                    // check that all labels are present between source and destination (edges could be one or more)
                    if (!dataGraph.hasEdgesWithLabels(sourceVertexId, destVertexId, edgeLabels)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
