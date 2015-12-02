package io.arabesque.testutils.graphs

import io.arabesque.graph.Edge
import io.arabesque.graph.Vertex
import io.arabesque.pattern.VertexPositionEquivalences

/**
 * A multi-label graph with 1 triangle, 1 square and a 4-pronged star.
 *
 * Graphical representation:
 *
 *     1 - 0
 *     |   | \
 * 2 - 0 - 2 - 3
 *     |
 *     1
 */
class LabelledTestGraph extends TestGraph {
    LabelledTestGraph() {
        super("test-labelled");

        addVertex(new Vertex(0, 0))
        addVertex(new Vertex(1, 1))
        addVertex(new Vertex(2, 2))
        addVertex(new Vertex(3, 1))
        addVertex(new Vertex(4, 2))
        addVertex(new Vertex(5, 0))
        addVertex(new Vertex(6, 3))

        addEdge(new Edge(0, 0, 1))
        addEdge(new Edge(1, 0, 2))
        addEdge(new Edge(2, 0, 3))
        addEdge(new Edge(3, 0, 4))
        addEdge(new Edge(4, 3, 5))
        addEdge(new Edge(5, 4, 5))
        addEdge(new Edge(6, 4, 6))
        addEdge(new Edge(7, 5, 6))
    }

    @Override
    Map<List<Integer>, VertexPositionEquivalences> getVEquivMap() {
        return VEQUIV_MAP
    }

    /**
     * Positions (and labels):
     *
     * <empty>
     */
    final static VertexPositionEquivalences VEQUIV_EMPTY_LABELLED =
            new VertexPositionEquivalences([:])

    /**
     * Positions (and labels):
     *
     * 0(0)
     */
    final static VertexPositionEquivalences VEQUIV_SINGLE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
            ])

    /**
     * Positions (and labels):
     *
     * 1(2) - 0(0) - 2(2) - 3(3)
     */
    final static VertexPositionEquivalences VEQUIV_PATH_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
            ])

    /**
     * Positions (and labels):
     *
     *     2(1) - 3(0)
     *     |
     *     0(0)
     *     |
     *     1(1)
     *
     */
    final static VertexPositionEquivalences VEQUIV_PATH_ALT_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
            ])

    /**
     *  Positions (and labels):
     *
     *        3(1)
     *        |
     * 2(2) - 0(0) - 4(2)
     *        |
     *        1(1)
     */
    final static VertexPositionEquivalences VEQUIV_STAR_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1, 3],
                    2: [2, 4],
                    3: [1, 3],
                    4: [2, 4]
            ])

    /**
     * Positions (and labels):
     *
     *  1(0)
     *  |   \
     *  0(2) - 2(3)
     */
    final static VertexPositionEquivalences VEQUIV_TRIANGLE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
            ])

    /**
     *  Positions (and labels):
     *
     *  1(1) - 3(0)
     *  |      |
     *  0(0) - 2(2)
     */
    final static VertexPositionEquivalences VEQUIV_SQUARE_LABELLED =
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
    final static VertexPositionEquivalences VEQUIV_COMPLETE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
                    4: [4],
                    5: [5],
                    6: [6]
            ])

    final static Map<List<Integer>, VertexPositionEquivalences> VEQUIV_MAP = [
            (EMBEDDING_EMPTY)            : VEQUIV_EMPTY_LABELLED,
            (EMBEDDING_SINGLE_VERTICES)  : VEQUIV_SINGLE_LABELLED,
            (EMBEDDING_PATH_VERTICES)    : VEQUIV_PATH_LABELLED,
            (EMBEDDING_PATH_ALT_VERTICES): VEQUIV_PATH_ALT_LABELLED,
            (EMBEDDING_STAR_VERTICES)    : VEQUIV_STAR_LABELLED,
            (EMBEDDING_TRIANGLE_VERTICES): VEQUIV_TRIANGLE_LABELLED,
            (EMBEDDING_SQUARE_VERTICES)  : VEQUIV_SQUARE_LABELLED,
            (EMBEDDING_COMPLETE_VERTICES): VEQUIV_COMPLETE_LABELLED
    ]

}
