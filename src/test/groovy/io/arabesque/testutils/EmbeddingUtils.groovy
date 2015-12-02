package io.arabesque.testutils

import io.arabesque.embedding.EdgeInducedEmbedding
import io.arabesque.embedding.Embedding
import io.arabesque.embedding.VertexInducedEmbedding
import io.arabesque.graph.MainGraph

class EmbeddingUtils {
    static void checkEmbeddingEmpty(Embedding embedding) {
        assert embedding.getVertices().isEmpty()
        assert embedding.getNumVertices() == 0
        assert embedding.getEdges().isEmpty()
        assert embedding.getNumEdges() == 0
        assert embedding.getNumEdgesAddedWithExpansion() == 0
        assert embedding.getNumVerticesAddedWithExpansion() == 0
        PatternUtils.checkPatternEmpty(embedding.getPattern())
    }

    static Embedding createVertexEmbedding(List<Integer> vertexIds) {
        VertexInducedEmbedding vertexInducedEmbedding = new VertexInducedEmbedding()

        for (Integer vertexId : vertexIds) {
            vertexInducedEmbedding.addWord(vertexId)
        }

        return vertexInducedEmbedding
    }

    static Embedding createEdgeEmbedding(List<Integer> edgeIds) {
        EdgeInducedEmbedding edgeInducedEmbedding = new EdgeInducedEmbedding()

        for (Integer edgeId : edgeIds) {
            edgeInducedEmbedding.addWord(edgeId)
        }

        return edgeInducedEmbedding
    }

    static List<List<Integer>> getValidVertexIdPermutations(MainGraph mainGraph, List<Integer> vertexIds) {
        if (vertexIds.isEmpty()) {
            return [[]]
        }

        def validPermutations = []

        vertexIds.eachPermutation { formConnectedEmbeddingVertexIds(mainGraph, it) ? validPermutations << it : null }

        return validPermutations
    }

    static boolean formConnectedEmbeddingVertexIds(MainGraph mainGraph, List<Integer> vertexIds) {
        List<Integer> vertexIdsLookedAt = []

        for (Integer v1 : vertexIds) {
            boolean foundConnectionToPrevious = false

            for (Integer v2 : vertexIdsLookedAt) {
                if (mainGraph.isNeighborVertex(v1, v2)) {
                    foundConnectionToPrevious = true
                }
            }

            if (foundConnectionToPrevious || vertexIdsLookedAt.isEmpty()) {
                vertexIdsLookedAt.add(v1)
            }
            else {
                return false;
            }
        }

        return true;
    }

    static List<List<Integer>> getValidEdgeIdPermutations(MainGraph mainGraph, List<Integer> edgeIds) {
        if (edgeIds.isEmpty()) {
            return [[]]
        }

        def validPermutations = []

        edgeIds.eachPermutation { formConnectedEmbeddingEdgeIds(mainGraph, it) ? validPermutations << it : null }

        return validPermutations
    }

    static boolean formConnectedEmbeddingEdgeIds(MainGraph mainGraph, List<Integer> edgeIds) {
        List<Integer> edgeIdsLookedAt = []

        for (Integer e1 : edgeIds) {
            boolean foundConnectionToPrevious = false

            for (Integer e2 : edgeIdsLookedAt) {
                if (mainGraph.areEdgesNeighbors(e1, e2)) {
                    foundConnectionToPrevious = true
                }
            }

            if (foundConnectionToPrevious || edgeIdsLookedAt.isEmpty()) {
                edgeIdsLookedAt.add(e1)
            }
            else {
                return false;
            }
        }

        return true;
    }

}
