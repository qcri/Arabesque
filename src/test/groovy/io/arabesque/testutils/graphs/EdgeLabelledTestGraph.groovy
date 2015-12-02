package io.arabesque.testutils.graphs

import io.arabesque.graph.LabelledEdge
import io.arabesque.graph.Vertex
import io.arabesque.pattern.VertexPositionEquivalences

/**
 * A vertex-labeled + edge-labelled graph with 1 triangle, 1 square and a 4-pronged star.
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
 */
class EdgeLabelledTestGraph extends TestGraph {
    EdgeLabelledTestGraph() {
        super("test-edge-labelled", true, false);

        addVertex(new Vertex(0, 0))
        addVertex(new Vertex(1, 1))
        addVertex(new Vertex(2, 2))
        addVertex(new Vertex(3, 1))
        addVertex(new Vertex(4, 2))
        addVertex(new Vertex(5, 0))
        addVertex(new Vertex(6, 3))

        addEdge(new LabelledEdge(0, 0, 1, 0))
        addEdge(new LabelledEdge(1, 0, 2, 1))
        addEdge(new LabelledEdge(2, 0, 3, 0))
        addEdge(new LabelledEdge(3, 0, 4, 1))
        addEdge(new LabelledEdge(4, 3, 5, 2))
        addEdge(new LabelledEdge(5, 4, 5, 2))
        addEdge(new LabelledEdge(6, 4, 6, 2))
        addEdge(new LabelledEdge(7, 5, 6, 3))
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
    final static VertexPositionEquivalences VEQUIV_EMPTY_EDGE_LABELLED =
            new VertexPositionEquivalences([:])

    /**
     * Positions (and labels):
     *
     * 0(0)
     */
    final static VertexPositionEquivalences VEQUIV_SINGLE_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
            ])

    /**
     * Positions (and labels):
     *
     * 1(2) -(1)-  0(0) -(1)- 2(2) -(3)- 3(3)
     */
    final static VertexPositionEquivalences VEQUIV_PATH_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
            ])

    /**
     * Positions (and labels):
     *
     *     2(1) -(2)- 3(0)
     *     |(0)
     *     0(0)
     *     |(0)
     *     1(1)
     *
     */
    final static VertexPositionEquivalences VEQUIV_PATH_ALT_EDGE_LABELLED =
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
    final static VertexPositionEquivalences VEQUIV_STAR_EDGE_LABELLED =
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
     *  |    \
     *  |(2)  (2)
     *  |        \
     *  0(2) -(3)- 2(3)
     */
    final static VertexPositionEquivalences VEQUIV_TRIANGLE_EDGE_LABELLED =
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
    final static VertexPositionEquivalences VEQUIV_SQUARE_EDGE_LABELLED =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3]
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
    final static VertexPositionEquivalences VEQUIV_COMPLETE_EDGE_LABELLED =
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
            (EMBEDDING_EMPTY)            : VEQUIV_EMPTY_EDGE_LABELLED,
            (EMBEDDING_SINGLE_VERTICES)  : VEQUIV_SINGLE_EDGE_LABELLED,
            (EMBEDDING_PATH_VERTICES)    : VEQUIV_PATH_EDGE_LABELLED,
            (EMBEDDING_PATH_ALT_VERTICES): VEQUIV_PATH_ALT_EDGE_LABELLED,
            (EMBEDDING_STAR_VERTICES)    : VEQUIV_STAR_EDGE_LABELLED,
            (EMBEDDING_TRIANGLE_VERTICES): VEQUIV_TRIANGLE_EDGE_LABELLED,
            (EMBEDDING_SQUARE_VERTICES)  : VEQUIV_SQUARE_EDGE_LABELLED,
            (EMBEDDING_COMPLETE_VERTICES): VEQUIV_COMPLETE_EDGE_LABELLED
    ]
}
