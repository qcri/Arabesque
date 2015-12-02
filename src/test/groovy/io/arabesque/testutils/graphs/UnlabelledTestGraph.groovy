package io.arabesque.testutils.graphs

import io.arabesque.graph.Edge
import io.arabesque.graph.Vertex
import io.arabesque.pattern.VertexPositionEquivalences

/**
 * A single-label graph with 1 triangle, 1 square and a 4-pronged star.
 *
 * Graphical representation:
 *
 *     1 - 1
 *     |   | \
 * 1 - 1 - 1 - 1
 *     |
 *     1
 */
class UnlabelledTestGraph extends TestGraph {
    UnlabelledTestGraph() {
        super("test-unlabelled");

        addVertex(new Vertex(0, 1))
        addVertex(new Vertex(1, 1))
        addVertex(new Vertex(2, 1))
        addVertex(new Vertex(3, 1))
        addVertex(new Vertex(4, 1))
        addVertex(new Vertex(5, 1))
        addVertex(new Vertex(6, 1))

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
    final static VertexPositionEquivalences VEQUIV_EMPTY_UNLABELLED =
            new VertexPositionEquivalences([:])

    /**
     * Positions (and labels):
     *
     * 0(1)
     */
    final static VertexPositionEquivalences VEQUIV_SINGLE_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0],
            ])

    /**
     * Positions (and labels):
     *
     * 1(1) - 0(1) - 2(1) - 3(1)
     */
    final static VertexPositionEquivalences VEQUIV_PATH_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0, 2],
                    1: [1, 3],
                    2: [0, 2],
                    3: [1, 3],
            ])

    /**
     * Positions (and labels):
     *
     *     2(1) - 3(1)
     *     |
     *     0(1)
     *     |
     *     1(1)
     *
     */
    final static VertexPositionEquivalences VEQUIV_PATH_ALT_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0, 2],
                    1: [1, 3],
                    2: [0, 2],
                    3: [1, 3],
            ])

    /**
     *  Positions (and labels):
     *
     *        3(1)
     *        |
     * 2(1) - 0(1) - 4(1)
     *        |
     *        1(1)
     */
    final static VertexPositionEquivalences VEQUIV_STAR_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1, 2, 3, 4],
                    2: [1, 2, 3, 4],
                    3: [1, 2, 3, 4],
                    4: [1, 2, 3, 4]
            ])

    /**
     * Positions (and labels):
     *
     *  1(1)
     *  |   \
     *  0(1) - 2(1)
     */
    final static VertexPositionEquivalences VEQUIV_TRIANGLE_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0, 1, 2],
                    1: [0, 1, 2],
                    2: [0, 1, 2],
            ])

    /**
     *  Positions (and labels):
     *
     *  1(1) - 3(1)
     *  |      |
     *  0(1) - 2(1)
     */
    final static VertexPositionEquivalences VEQUIV_SQUARE_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0, 1, 2, 3],
                    1: [0, 1, 2, 3],
                    2: [0, 1, 2, 3],
                    3: [0, 1, 2, 3]
            ])

    /**
     * Positions (and labels):
     *
     *        3(1) - 5(1)
     *        |      |    \
     * 2(1) - 0(1) - 4(1) - 6(1)
     *        |
     *        1(1)
     */
    final static VertexPositionEquivalences VEQUIV_COMPLETE_UNLABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1, 2],
                    2: [1, 2],
                    3: [3],
                    4: [4],
                    5: [5],
                    6: [6]
            ])

    final static Map<List<Integer>, VertexPositionEquivalences> VEQUIV_MAP = [
            (EMBEDDING_EMPTY)            : VEQUIV_EMPTY_UNLABELLED,
            (EMBEDDING_SINGLE_VERTICES)  : VEQUIV_SINGLE_UNLABELLED,
            (EMBEDDING_PATH_VERTICES)    : VEQUIV_PATH_UNLABELLED,
            (EMBEDDING_PATH_ALT_VERTICES): VEQUIV_PATH_ALT_UNLABELLED,
            (EMBEDDING_STAR_VERTICES)    : VEQUIV_STAR_UNLABELLED,
            (EMBEDDING_TRIANGLE_VERTICES): VEQUIV_TRIANGLE_UNLABELLED,
            (EMBEDDING_SQUARE_VERTICES)  : VEQUIV_SQUARE_UNLABELLED,
            (EMBEDDING_COMPLETE_VERTICES): VEQUIV_COMPLETE_UNLABELLED
    ]
}
