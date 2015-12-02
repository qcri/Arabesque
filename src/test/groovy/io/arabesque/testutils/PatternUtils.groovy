package io.arabesque.testutils

import io.arabesque.conf.Configuration
import io.arabesque.pattern.Pattern

class PatternUtils {
    static void checkPatternEmpty(Pattern pattern) {
        assert pattern.getNumberOfEdges() == 0
        assert pattern.getNumberOfVertices() == 0
        assert pattern.getEdges().isEmpty()
        assert pattern.getVertices().isEmpty()
        assert pattern.getCanonicalLabeling().isEmpty()
        assert pattern.getVertexPositionEquivalences().isEmpty()
    }

    static Pattern createPattern() {
        return Configuration.get().createPattern()
    }

    static Pattern createPatternFromVertexIds(List<Integer> vertexIds) {
        Pattern pattern = createPattern()
        resetPatternFromVertexIds(pattern, vertexIds)
        return pattern
    }

    static Pattern createPatternFromEdgeIds(List<Integer> edgeIds) {
        Pattern pattern = createPattern()
        resetPatternFromEdgeIds(pattern, edgeIds)
        return pattern
    }

    static void resetPatternFromVertexIds(Pattern pattern, List<Integer> vertexIds) {
        pattern.setEmbedding(EmbeddingUtils.createVertexEmbedding(vertexIds))
    }

    static void resetPatternFromEdgeIds(Pattern pattern, List<Integer> edgeIds) {
        pattern.setEmbedding(EmbeddingUtils.createEdgeEmbedding(edgeIds))
    }
}
