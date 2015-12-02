package io.arabesque.embedding

import io.arabesque.testutils.EmbeddingUtils
import io.arabesque.testutils.graphs.TestGraph
import spock.lang.Unroll

@Unroll
class VertexInducedEmbeddingSpec extends EmbeddingSpec {

    // On all the ...throughout modifications... tests, we start by constructing a star
    // and then morph it into a square

    //@Override
    def "Embedding structure should remain correct throughout modifications of that embedding in a simple graph"() {
        given: "a simple labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add vertex 0"
        embedding.addWord(0)
        then: "we should have 1 vertex and 0 edges"
        embedding.getNumWords() == 1
        embedding.getNumVertices() == 1
        embedding.getNumEdges() == 0
        embedding.getVertices().asList() == [0]
        embedding.getEdges().isEmpty()

        when: "we add vertex 3"
        embedding.addWord(3)
        then: "we should have 2 vertices and 1 edge connecting them"
        embedding.getNumWords() == 2
        embedding.getNumVertices() == 2
        embedding.getNumEdges() == 1
        embedding.getVertices().asList() == [0, 3]
        embedding.getEdges().asList() == [2]

        when: "we add vertex 4"
        embedding.addWord(4)
        then: "we should have 3 vertices and 2 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 3]

        when: "we add vertex 1"
        embedding.addWord(1)
        then: "we should have 4 vertices and 3 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 3
        embedding.getVertices().asList() == [0, 3, 4, 1]
        embedding.getEdges().asList() == [2, 3, 0]

        when: "we add vertex 2"
        embedding.addWord(2)
        then: "we should have 5 vertices and 4 edges connecting them"
        embedding.getNumWords() == 5
        embedding.getNumVertices() == 5
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4, 1, 2]
        embedding.getEdges().asList() == [2, 3, 0, 1]

        // we now have a star

        when: "we remove last 2 vertices"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "we should have 3 vertices and 2 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 3]

        when: "we add vertex 5"
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

        when: "we add vertex 0"
        embedding.addWord(0)
        then: "we should have 1 vertex and 0 edges"
        embedding.getNumWords() == 1
        embedding.getNumVertices() == 1
        embedding.getNumEdges() == 0
        embedding.getVertices().asList() == [0]
        embedding.getEdges().isEmpty()

        when: "we add vertex 3"
        embedding.addWord(3)
        then: "we should have 2 vertices and 2 edges connecting them"
        embedding.getNumWords() == 2
        embedding.getNumVertices() == 2
        embedding.getNumEdges() == 2
        embedding.getVertices().asList() == [0, 3]
        embedding.getEdges().asList() == [2, 8]

        when: "we add vertex 4"
        embedding.addWord(4)
        then: "we should have 3 vertices and 4 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 8, 3, 9]

        when: "we add vertex 1"
        embedding.addWord(1)
        then: "we should have 4 vertices and 5 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 5
        embedding.getVertices().asList() == [0, 3, 4, 1]
        embedding.getEdges().asList() == [2, 8, 3, 9, 0]

        when: "we add vertex 2"
        embedding.addWord(2)
        then: "we should have 5 vertices and 6 edges connecting them"
        embedding.getNumWords() == 5
        embedding.getNumVertices() == 5
        embedding.getNumEdges() == 6
        embedding.getVertices().asList() == [0, 3, 4, 1, 2]
        embedding.getEdges().asList() == [2, 8, 3, 9, 0, 1]

        // we now have a star

        when: "we remove last 2 vertices"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "we should have 3 vertices and 4 edges connecting them"
        embedding.getNumWords() == 3
        embedding.getNumVertices() == 3
        embedding.getNumEdges() == 4
        embedding.getVertices().asList() == [0, 3, 4]
        embedding.getEdges().asList() == [2, 8, 3, 9]

        when: "we add vertex 5"
        embedding.addWord(5)
        then: "we should have 4 vertices and 4 edges connecting them"
        embedding.getNumWords() == 4
        embedding.getNumVertices() == 4
        embedding.getNumEdges() == 8
        embedding.getVertices().asList() == [0, 3, 4, 5]
        embedding.getEdges().asList() == [2, 8, 3, 9, 4, 10, 5, 11]

        // we now have a square
    }

    //@Override
    def "Embedding extensions should remain correct throughout modifications of that embedding in a simple graph"() {
        given: "a simple labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add vertex 0"
        embedding.addWord(0)
        then: "extensions should be all the other vertices belonging to the star"
        embedding.getExtensibleWordIds().sort() == [1, 2, 3, 4]

        when: "we add vertex 3"
        embedding.addWord(3)
        then: "extensions should no longer include 3 and should now include 5, the corner of the square"
        embedding.getExtensibleWordIds().sort() == [1, 2, 4, 5]

        when: "we add vertex 4"
        embedding.addWord(4)
        then: "extensions should no longer include 4 and should now include 6, the corner of the triangle"
        embedding.getExtensibleWordIds().sort() == [1, 2, 5, 6]

        when: "we add vertex 1"
        embedding.addWord(1)
        then: "extensions should no longer include 1"
        embedding.getExtensibleWordIds().sort() == [2, 5, 6]

        when: "we add vertex 2"
        embedding.addWord(2)
        then: "extensions should no longer include 2"
        embedding.getExtensibleWordIds().sort() == [5, 6]

        // we now have a star

        when: "we remove last 2 vertices"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "extensions should go back to the same as when we added 4"
        embedding.getExtensibleWordIds().sort() == [1, 2, 5, 6]

        when: "we add vertex 5"
        embedding.addWord(5)
        then: "extensions should no longer have 5"
        embedding.getExtensibleWordIds().sort() == [1, 2, 6]

        // we now have a square
    }

    //@Override
    def "Embedding extensions should remain correct throughout modifications of that embedding in a multi graph"() {
        given: "a multi labelled graph"
        setMainGraph(TEST_GRAPH_MULTI)
        and: "an empty embedding"
        Embedding embedding = createEmbedding()

        when: "we add vertex 0"
        embedding.addWord(0)
        then: "extensions should be all the other vertices belonging to the star"
        embedding.getExtensibleWordIds().sort() == [1, 2, 3, 4]

        when: "we add vertex 3"
        embedding.addWord(3)
        then: "extensions should no longer include 3 and should now include 5, the corner of the square"
        embedding.getExtensibleWordIds().sort() == [1, 2, 4, 5]

        when: "we add vertex 4"
        embedding.addWord(4)
        then: "extensions should no longer include 4 and should now include 6, the corner of the triangle"
        embedding.getExtensibleWordIds().sort() == [1, 2, 5, 6]

        when: "we add vertex 1"
        embedding.addWord(1)
        then: "extensions should no longer include 1"
        embedding.getExtensibleWordIds().sort() == [2, 5, 6]

        when: "we add vertex 2"
        embedding.addWord(2)
        then: "extensions should no longer include 2"
        embedding.getExtensibleWordIds().sort() == [5, 6]

        // we now have a star

        when: "we remove last 2 vertices"
        embedding.removeLastWord()
        embedding.removeLastWord()
        then: "extensions should go back to the same as when we added 4"
        embedding.getExtensibleWordIds().sort() == [1, 2, 5, 6]

        when: "we add vertex 5"
        embedding.addWord(5)
        then: "extensions should no longer have 5"
        embedding.getExtensibleWordIds().sort() == [1, 2, 6]

        // we now have a square
    }

    @Override
    Embedding createEmbedding() {
        return new VertexInducedEmbedding()
    }

    @Override
    List<Integer> getWordIdsFromEmbeddingId(TestGraph graph, TestGraph.EmbeddingId embeddingId) {
        return graph.getVertexEmbeddingMap().get(embeddingId)
    }

    @Override
    List<List<Integer>> getValidWordIdsPermutations(TestGraph graph, List<Integer> wordIds) {
        return EmbeddingUtils.getValidVertexIdPermutations(graph, wordIds)
    }

    @Override
    List<TestGraph.EmbeddingId> getGraphEmbeddingIds(TestGraph graph) {
        return graph.getVertexEmbeddingMap().keySet().asList()
    }
}
