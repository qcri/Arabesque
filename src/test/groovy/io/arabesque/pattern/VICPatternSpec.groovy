package io.arabesque.pattern

import io.arabesque.graph.MainGraph
import io.arabesque.testutils.EmbeddingUtils
import io.arabesque.testutils.graphs.EdgeLabelledMultiTestGraph
import io.arabesque.testutils.graphs.EdgeLabelledTestGraph
import io.arabesque.testutils.graphs.TestGraph
import spock.lang.Ignore
import spock.lang.Shared

class VICPatternSpec extends PatternSpec {
    @Shared static final TestGraph TEST_GRAPH_EDGE_LABELLED = new EdgeLabelledTestGraph()
    @Shared static final TestGraph TEST_GRAPH_EDGE_LABELLED_MULTI = new EdgeLabelledMultiTestGraph()

    @Override
    Pattern createPattern() {
        return new VICPattern();
    }

    @Override
    List<MainGraph> createTestGraphs() {
        return super.createTestGraphs() + [
                TEST_GRAPH_EDGE_LABELLED,
                TEST_GRAPH_EDGE_LABELLED_MULTI
        ]
    }

    def "VICPattern should only find canonical labelling once when asked to perform canonical-labelling operations in a sequence"() {
        given: "a labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "a path pattern on that graph"
        VICPattern pattern = Spy(VICPattern)
        pattern.setEmbedding(EmbeddingUtils.createVertexEmbedding(TEST_GRAPH_LABELLED.EMBEDDING_PATH_VERTICES))

        when: "we calculate canonical labelling"
        pattern.getCanonicalLabeling()
        then: "_findCanonicaLabelling should have been called 1 or more times"
        (1.._) * pattern._findCanonicalLabelling(_)

        when: "we calculate vertex position equivalences"
        pattern.getVertexPositionEquivalences()
        then: "it should reuse previously calculated canonical labelling"
        0 * pattern._findCanonicalLabelling(_)

        when: "we turn the pattern canonical"
        pattern.turnCanonical()
        then: "it should reuse previously calculated canonical labelling"
        0 * pattern._findCanonicalLabelling(_)
    }




    @Ignore
    def playground() {
        setup:
        def equiv = new VertexPositionEquivalences([:])
        println equiv
        setMainGraph(TEST_GRAPH_LABELLED)
        Pattern pattern = createPatternFromVertexIds([3, 5, 4, 0])

        expect:
        pattern.getVertexPositionEquivalences() == new VertexPositionEquivalences([0: [0], 1: [1, 3], 2: [2], 3: [1,3]])
    }
}
