package io.arabesque.pattern

import io.arabesque.graph.BasicMainGraph
import io.arabesque.graph.LabelledEdge
import io.arabesque.graph.MainGraph
import io.arabesque.graph.Vertex
import spock.lang.Ignore
import spock.lang.Shared

class VICPatternSpec extends PatternSpec {
    @Shared static final MainGraph TEST_GRAPH_EDGE_LABELLED = createEdgeLabelledTestGraph()
    @Shared static final MainGraph TEST_GRAPH_EDGE_LABELLED_MULTI = createEdgeLabelledMultiTestGraph()

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

    @Override
    Map<MainGraph, Map<List<Integer>, VertexPositionEquivalences>> createGraphToEmbeddingToExpectedEquivMap() {
        return super.createGraphToEmbeddingToExpectedEquivMap() + [
                (TEST_GRAPH_EDGE_LABELLED): [
                        (TEST_EMBEDDING_PATH_VERTICES): TEST_VEQUIV_PATH_EDGE_LABELLED,
                        (TEST_EMBEDDING_STAR_VERTICES): TEST_VEQUIV_STAR_EDGE_LABELLED,
                        (TEST_EMBEDDING_TRIANGLE_VERTICES): TEST_VEQUIV_TRIANGLE_EDGE_LABELLED,
                        (TEST_EMBEDDING_SQUARE_VERTICES): TEST_VEQUIV_SQUARE_EDGE_LABELLED,
                        (TEST_EMBEDDING_COMPLETE_VERTICES): TEST_VEQUIV_COMPLETE_EDGE_LABELLED
                ],
                (TEST_GRAPH_EDGE_LABELLED_MULTI): [
                        (TEST_EMBEDDING_PATH_VERTICES): TEST_VEQUIV_PATH_EDGE_LABELLED_MULTI,
                        (TEST_EMBEDDING_STAR_VERTICES): TEST_VEQUIV_STAR_EDGE_LABELLED_MULTI,
                        (TEST_EMBEDDING_TRIANGLE_VERTICES): TEST_VEQUIV_TRIANGLE_EDGE_LABELLED_MULTI,
                        (TEST_EMBEDDING_SQUARE_VERTICES): TEST_VEQUIV_SQUARE_EDGE_LABELLED_MULTI,
                        (TEST_EMBEDDING_COMPLETE_VERTICES): TEST_VEQUIV_COMPLETE_EDGE_LABELLED_MULTI
                ]
        ]
    }

    def "VICPattern should only find canonical labelling once when asked to perform canonical-labelling operations in a sequence"() {
        given: "a multi labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "a path pattern on that graph"
        VICPattern pattern = Spy(VICPattern)
        pattern.setEmbedding(createVertexEmbedding(TEST_EMBEDDING_PATH_VERTICES))

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

    /**
     * Creates a vertex-labeled + edge-labelled graph with 1 triangle, 1 square and a 4-pronged star.
     *
     * Graphical representation:
     *
     *            (2)     (2)
     *             |     /
     *           1 - 0  /
     * (0)-------|   |-\--------(2)
     *       2 - 0 - 2 - 3
     *         | |-+---+-------(0)
     *         | 1 |   |
     *       (1)   |   |
     *            (1)  (3)
     * @return Maingraph with multiple labels.
     */
    static MainGraph createEdgeLabelledTestGraph() {
        MainGraph mainGraph = new BasicMainGraph("test-edge-labelled", true, false)

        mainGraph.addVertex(new Vertex(0, 0))
        mainGraph.addVertex(new Vertex(1, 1))
        mainGraph.addVertex(new Vertex(2, 2))
        mainGraph.addVertex(new Vertex(3, 1))
        mainGraph.addVertex(new Vertex(4, 2))
        mainGraph.addVertex(new Vertex(5, 0))
        mainGraph.addVertex(new Vertex(6, 3))

        mainGraph.addEdge(new LabelledEdge(0, 0, 1, 0))
        mainGraph.addEdge(new LabelledEdge(1, 0, 2, 1))
        mainGraph.addEdge(new LabelledEdge(2, 0, 3, 0))
        mainGraph.addEdge(new LabelledEdge(3, 0, 4, 1))
        mainGraph.addEdge(new LabelledEdge(4, 3, 5, 2))
        mainGraph.addEdge(new LabelledEdge(5, 4, 5, 2))
        mainGraph.addEdge(new LabelledEdge(6, 4, 6, 2))
        mainGraph.addEdge(new LabelledEdge(7, 5, 6, 3))

        return mainGraph
    }

    /**
     * Creates a vertex-labeled + edge-labelled multigraph with 1 triangle, 1 square and a 4-pronged star.
     *
     * Graphical representation:
     *
     *            (0,2)   (2)
     *             |     /
     *           1 - 0  /
     * (0,2)-----|   |-\--------(1,2)
     *       2 - 0 - 2 - 3
     *         | |-+---+-------(0)
     *         | 1 |   |
     *        (1)  |   |
     *           (1,2) (3)
     * @return Maingraph with multiple labels.
     */
    static MainGraph createEdgeLabelledMultiTestGraph() {
        MainGraph mainGraph = new BasicMainGraph("test-edge-labelled-multi", true, true)

        mainGraph.addVertex(new Vertex(0, 0))
        mainGraph.addVertex(new Vertex(1, 1))
        mainGraph.addVertex(new Vertex(2, 2))
        mainGraph.addVertex(new Vertex(3, 1))
        mainGraph.addVertex(new Vertex(4, 2))
        mainGraph.addVertex(new Vertex(5, 0))
        mainGraph.addVertex(new Vertex(6, 3))

        mainGraph.addEdge(new LabelledEdge(0, 0, 1, 0))
        mainGraph.addEdge(new LabelledEdge(1, 0, 2, 1))
        mainGraph.addEdge(new LabelledEdge(2, 0, 3, 0))
        mainGraph.addEdge(new LabelledEdge(3, 0, 4, 1))
        mainGraph.addEdge(new LabelledEdge(4, 3, 5, 2))
        mainGraph.addEdge(new LabelledEdge(5, 4, 5, 2))
        mainGraph.addEdge(new LabelledEdge(6, 4, 6, 2))
        mainGraph.addEdge(new LabelledEdge(7, 5, 6, 3))

        mainGraph.addEdge(new LabelledEdge(8, 0, 3, 2))
        mainGraph.addEdge(new LabelledEdge(9, 0, 4, 2))
        mainGraph.addEdge(new LabelledEdge(10, 3, 5, 0))
        mainGraph.addEdge(new LabelledEdge(11, 4, 5, 1))

        return mainGraph
    }

    /**
     * Positions (and labels):
     *
     * 1(2) -(1)-  0(0) -(1)- 2(2) -(3)- 3(3)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_PATH_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
            ])

    /**
     * Positions (and labels):
     *
     * 1(2) -(1)-  0(0) -(1,2)- 2(2) -(3)- 3(3)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_PATH_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
            ])

    /**
     *  Positions (and labels):
     *
     *            3(1)
     *            |(0)
     * 2(1) -(1)- 0(1) -(1)- 4(1)
     *            |(0)
     *            1(1)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_STAR_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1, 3],
                    2: [2, 4],
                    3: [1, 3],
                    4: [2, 4]
            ])

    /**
     *  Positions (and labels):
     *
     *            3(1)
     *            |(0,2)
     * 2(1) -(1)- 0(1) -(1,2)- 4(1)
     *            |(0)
     *            1(1)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_STAR_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
                    4: [4]
            ])

    /**
     * Positions (and labels):
     *
     *  1(0)
     *  |    \
     *  |(2)  (2)
     *  |        \
     *  0(2) -(3)- 2(3)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_TRIANGLE_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
            ])

    /**
     * Positions (and labels):
     *
     *  1(0)
     *  |    \
     *  |(1,2)(2)
     *  |        \
     *  0(2) -(3)- 2(3)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_TRIANGLE_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
            ])

    /**
     *  Positions (and labels):
     *
     *  1(1) -(2)- 3(0)
     *  |(0)       |(2)
     *  0(0) -(1)- 2(2)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_SQUARE_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3]
            ])

    /**
     *  Positions (and labels):
     *
     *  1(1) -(0,2)- 3(0)
     *  |(0,2)       |(1,2)
     *  0(0) -(1,2)- 2(2)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_SQUARE_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0, 3],
                    1: [1],
                    2: [2],
                    3: [0, 3]
            ])

    /**
     * Positions (and labels):
     *
     *        3(1) - 5(0)
     *        |      |    \
     * 2(2) - 0(0) - 4(2) - 6(3)
     *        |
     *        1(1)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_COMPLETE_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
                    4: [4],
                    5: [5],
                    6: [6]
            ])

    /**
     * Positions (and labels):
     *
     *        3(1) - 5(0)
     *        |      |    \
     * 2(2) - 0(0) - 4(2) - 6(3)
     *        |
     *        1(1)
     */
    @Shared final static VertexPositionEquivalences TEST_VEQUIV_COMPLETE_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
                    4: [4],
                    5: [5],
                    6: [6]
            ])

    @Ignore
    def playground() {
        setup:
        setMainGraph(TEST_GRAPH_LABELLED)
        Pattern pattern = createPatternFromVertexIds([3, 5, 4, 0])

        expect:
        pattern.getVertexPositionEquivalences() == new VertexPositionEquivalences([0: [0], 1: [1, 3], 2: [2], 3: [1,3]])
    }
}
