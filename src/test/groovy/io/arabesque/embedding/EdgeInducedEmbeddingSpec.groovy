package io.arabesque.embedding

import io.arabesque.testutils.EmbeddingUtils
import io.arabesque.testutils.graphs.TestGraph
import spock.lang.Unroll

@Unroll
class EdgeInducedEmbeddingSpec extends EmbeddingSpec {

    // On all the ...throughout modifications... tests, we start by constructing a star
    // and then morph it into a square

    //@Override
    def "Embedding structure should remain correct throughout modifications of that embedding in a simple graph"() {
        given: "a simple labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add edge 2"
        embedding.addWord(2)
        then: "we should have 2 vertices and 1 edge"
        embedding.getNumWords() == 1
        embedding.getNumVertices() == 2
        embedding.getNumEdges() == 1
        embedding.getVertices().asList() == [0, 3]
        embedding.getEdges().asList() == [2]

        when: "we add edge 3"
        embedding.addWord(3)
        then: "we should have 3 vertices and 2 edges connecting them"
        embedding.getNumWords() == 2
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 3]

        when: "we add edge 0"
        embedding.addWord(0)
        then: "we should have 4 vertices and 3 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 3
        embedding.getVertices().asList() == [0, 3, 4, 1]
        embedding.getEdges().asList() == [2, 3, 0]

        when: "we add edge 1"
        embedding.addWord(1)
        then: "we should have 5 vertices and 4 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 5
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4, 1, 2]
        embedding.getEdges().asList() == [2, 3, 0, 1]

        // we now have a star

        when: "we remove last 2 edges"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "we should have 3 vertices and 2 edges connecting them"
        embedding.getNumWords() == 2
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 3]

        when: "we add edge 4"
        embedding.addWord(4)
        then: "we should have 4 vertices and 3 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 3
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 3, 4]

        when: "we add edge 5"
        embedding.addWord(5)
        then: "we should have 4 vertices and 4 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 3, 4, 5]

        // we now have a square
    }

    //@Override
    def "Embedding structure should remain correct throughout modifications of that embedding in a multi graph"() {
        given: "a multi labelled graph"
        setMainGraph(TEST_GRAPH_MULTI)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add edge 2"
        embedding.addWord(2)
        then: "we should have 2 vertices and 1 edge"
        embedding.getNumWords() == 1
        embedding.getNumVertices() == 2
        embedding.getNumEdges() == 1
        embedding.getVertices().asList() == [0, 3]
        embedding.getEdges().asList() == [2]

        when: "we add edge 3"
        embedding.addWord(3)
        then: "we should have 3 vertices and 2 edges connecting them"
        embedding.getNumWords() == 2
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 3]

        when: "we add edge 0"
        embedding.addWord(0)
        then: "we should have 4 vertices and 3 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 3
        embedding.getVertices().asList() == [0, 3, 4, 1]
        embedding.getEdges().asList() == [2, 3, 0]

        when: "we add edge 1"
        embedding.addWord(1)
        then: "we should have 5 vertices and 4 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 5
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4, 1, 2]
        embedding.getEdges().asList() == [2, 3, 0, 1]

        // we now have a star

        when: "we add edge 8"
        embedding.addWord(8)
        then: "we should have 5 vertices and 5 edges connecting them"
        embedding.getNumWords() == 5
        embedding.getNumVertices() == 5
        embedding.getNumEdges() == 5
        embedding.getVertices().asList() == [0, 3, 4, 1, 2]
        embedding.getEdges().asList() == [2, 3, 0, 1, 8]

        when: "we add edge 9"
        embedding.addWord(9)
        then: "we should have 5 vertices and 4 edges connecting them"
        embedding.getNumWords() == 6
        embedding.getNumVertices() == 5
        embedding.getNumEdges() == 6
        embedding.getVertices().asList() == [0, 3, 4, 1, 2]
        embedding.getEdges().asList() == [2, 3, 0, 1, 8, 9]

        // we now have a star with double edges between vertices 0-3 and 0-4

        when: "we remove last 4 edges"
        embedding.removeLastWord()
        embedding.removeLastWord()
        embedding.removeLastWord()
        embedding.removeLastWord()

        then: "we should have 3 vertices and 2 edges connecting them"
        embedding.getNumWords() == 2
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 3]

        when: "we add edge 4"
        embedding.addWord(4)
        then: "we should have 4 vertices and 3 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 3
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 3, 4]

        when: "we add edge 5"
        embedding.addWord(5)
        then: "we should have 4 vertices and 4 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 3, 4, 5]

        // we now have a square

        when: "we add edge 10"
        embedding.addWord(10)
        then: "we should have 4 vertices and 5 edges connecting them"
        embedding.getNumWords() == 5
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 5
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 3, 4, 5, 10]

        when: "we add edge 11"
        embedding.addWord(11)
        then: "we should have 4 vertices and 6 edges connecting them"
        embedding.getNumWords() == 6
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 6
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 3, 4, 5, 10, 11]

        // we now have a square with double edges between 3-5 and 4-5
    }

    //@Override
    def "Embedding extensions should remain correct throughout modifications of that embedding in a simple graph"() {
        given: "a simple labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add edge 3"
        embedding.addWord(3)
        then: "extensions should be all neighbour edges of 3"
        embedding.getExtensibleWordIds().sort() == [0, 1, 2, 5, 6]

        when: "we add edge 2"
        embedding.addWord(2)
        then: "extensions should no longer include 2 and should now include 4, the upper edge of the square"
        embedding.getExtensibleWordIds().sort() == [0, 1, 4, 5, 6]

        when: "we add edge 1"
        embedding.addWord(1)
        then: "extensions should no longer include 1"
        embedding.getExtensibleWordIds().sort() == [0, 4, 5, 6]

        when: "we add edge 0"
        embedding.addWord(0)
        then: "extensions should no longer include 0"
        embedding.getExtensibleWordIds().sort() == [4, 5, 6]

        // we now have a star

        when: "we remove last 2 edges"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "extensions should go back to the same as when we added 2"
        embedding.getExtensibleWordIds().sort() == [0, 1, 4, 5, 6]

        when: "we add edge 4"
        embedding.addWord(4)
        then: "extensions should no longer have 4 and it should now include the last edge of the triangle"
        embedding.getExtensibleWordIds().sort() == [0, 1, 5, 6, 7]

        when: "we add edge 5"
        embedding.addWord(5)
        then: "extensions should no longer have 5"
        embedding.getExtensibleWordIds().sort() == [0, 1, 6, 7]

        // we now have a square
    }

    //@Override
    def "Embedding extensions should remain correct throughout modifications of that embedding in a multi graph"() {
        given: "a multi labelled graph"
        setMainGraph(TEST_GRAPH_MULTI)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add edge 3"
        embedding.addWord(3)
        then: "extensions should be all neighbour edges of 3"
        embedding.getExtensibleWordIds().sort() == [0, 1, 2, 5, 6, 8, 9, 11]

        when: "we add edge 2"
        embedding.addWord(2)
        then: "extensions should no longer include 2 and should now include 4 and 10, the upper edges of the square"
        embedding.getExtensibleWordIds().sort() == [0, 1, 4, 5, 6, 8, 9, 10, 11]

        when: "we add edge 1"
        embedding.addWord(1)
        then: "extensions should no longer include 1"
        embedding.getExtensibleWordIds().sort() == [0, 4, 5, 6, 8, 9, 10, 11]

        when: "we add edge 0"
        embedding.addWord(0)
        then: "extensions should no longer include 0"
        embedding.getExtensibleWordIds().sort() == [4, 5, 6, 8, 9, 10, 11]

        // we now have a star

        when: "we remove last 2 edges"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "extensions should go back to the same as when we added 2"
        embedding.getExtensibleWordIds().sort() == [0, 1, 4, 5, 6, 8, 9, 10, 11]

        when: "we add edge 4"
        embedding.addWord(4)
        then: "extensions should no longer have 4 and it should now include the last edge of the triangle"
        embedding.getExtensibleWordIds().sort() == [0, 1, 5, 6, 7, 8, 9, 10, 11]

        when: "we add edge 5"
        embedding.addWord(5)
        then: "extensions should no longer have 5"
        embedding.getExtensibleWordIds().sort() == [0, 1, 6, 7, 8, 9, 10, 11]

        // we now have a square
    }

    @Override
    Embedding createEmbedding() {
        return new EdgeInducedEmbedding()
    }

    @Override
    List<Integer> getWordIdsFromEmbeddingId(TestGraph graph, TestGraph.EmbeddingId embeddingId) {
        return graph.getEdgeEmbeddingMap().get(embeddingId)
    }

    @Override
    List<List<Integer>> getValidWordIdsPermutations(TestGraph graph, List<Integer> wordIds) {
        return EmbeddingUtils.getValidEdgeIdPermutations(graph, wordIds)
    }

    @Override
    List<TestGraph.EmbeddingId> getGraphEmbeddingIds(TestGraph graph) {
        return graph.getEdgeEmbeddingMap().keySet().asList()
    }
}
