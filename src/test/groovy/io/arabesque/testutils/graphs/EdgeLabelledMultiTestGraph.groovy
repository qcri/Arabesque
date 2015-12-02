package io.arabesque.testutils.graphs

import io.arabesque.graph.LabelledEdge
import io.arabesque.graph.Vertex
import io.arabesque.pattern.VertexPositionEquivalences

/**
 * A vertex-labeled + edge-labelled multigraph with 1 triangle, 1 square and a 4-pronged star.
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
 */
class EdgeLabelledMultiTestGraph extends TestGraph {
    EdgeLabelledMultiTestGraph() {
        super("test-edge-labelled-multi", true, true);

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

        addEdge(new LabelledEdge(8, 0, 3, 2))
        addEdge(new LabelledEdge(9, 0, 4, 2))
        addEdge(new LabelledEdge(10, 3, 5, 0))
        addEdge(new LabelledEdge(11, 4, 5, 1))
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
    final static VertexPositionEquivalences VEQUIV_EMPTY_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([:])

    /**
     * Positions (and labels):
     *
     * 0(0)
     */
    final static VertexPositionEquivalences VEQUIV_SINGLE_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
            ])

    /**
     * Positions (and labels):
     *
     * 1(2) -(1)-  0(0) -(1,2)- 2(2) -(3)- 3(3)
     */
    final static VertexPositionEquivalences VEQUIV_PATH_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
                    3: [3],
            ])

    /**
     * Positions (and labels):
     *
     *     2(1) -(0,2)- 3(0)
     *     |(0,2)
     *     0(0)
     *     |(0)
     *     1(1)
     *
     */
    final static VertexPositionEquivalences VEQUIV_PATH_ALT_EDGE_LABELLED_MULTI =
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
     *            |(0,2)
     * 2(1) -(1)- 0(1) -(1,2)- 4(1)
     *            |(0)
     *            1(1)
     */
    final static VertexPositionEquivalences VEQUIV_STAR_EDGE_LABELLED_MULTI =
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
     *  |(1,2)(2)
     *  |        \
     *  0(2) -(3)- 2(3)
     */
    final static VertexPositionEquivalences VEQUIV_TRIANGLE_EDGE_LABELLED_MULTI =
            new VertexPositionEquivalences([
                    0: [0],
                    1: [1],
                    2: [2],
            ])

    /**
     *  Positions (and labels):
     *
     *  1(1) -(0,2)- 3(0)
     *  |(0,2)       |(1,2)
     *  0(0) -(1,2)- 2(2)
     */
    final static VertexPositionEquivalences VEQUIV_SQUARE_EDGE_LABELLED_MULTI =
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
    final static VertexPositionEquivalences VEQUIV_COMPLETE_EDGE_LABELLED_MULTI =
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
            (EMBEDDING_EMPTY)            : VEQUIV_EMPTY_EDGE_LABELLED_MULTI,
            (EMBEDDING_SINGLE_VERTICES)  : VEQUIV_SINGLE_EDGE_LABELLED_MULTI,
            (EMBEDDING_PATH_VERTICES)    : VEQUIV_PATH_EDGE_LABELLED_MULTI,
            (EMBEDDING_PATH_ALT_VERTICES): VEQUIV_PATH_ALT_EDGE_LABELLED_MULTI,
            (EMBEDDING_STAR_VERTICES)    : VEQUIV_STAR_EDGE_LABELLED_MULTI,
            (EMBEDDING_TRIANGLE_VERTICES): VEQUIV_TRIANGLE_EDGE_LABELLED_MULTI,
            (EMBEDDING_SQUARE_VERTICES)  : VEQUIV_SQUARE_EDGE_LABELLED_MULTI,
            (EMBEDDING_COMPLETE_VERTICES): VEQUIV_COMPLETE_EDGE_LABELLED_MULTI
    ]

    /**
     * Edges that compose a path edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     * # -(1)- # -(3,9)- # -(6)- #
     */
    final static List<Integer> EMBEDDING_PATH_EDGES = [1, 3, 6, 9]

    /**
     * Edges that compose an alternative path edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     # -(4,10)- #
     *     |(2,8)
     *     #
     *     |(0)
     *     #
     *
     */
    final static List<Integer> EMBEDDING_PATH_ALT_EDGES = [0, 2, 4, 8, 10]

    /**
     * Edges that compose a star edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *         #
     *         |(2,8)
     * # -(1)- # -(3,9) #
     *         |(0)
     *         #
     *
     */
    final static List<Integer> EMBEDDING_STAR_EDGES = [0, 1, 2, 3, 8, 9]

    /**
     * Edges that compose a triangle edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *         #
     *   (5,11)| \(7)
     *         # - #
     *           (6)
     *
     */
    final static List<Integer> EMBEDDING_TRIANGLE_EDGES = [5, 6, 7, 11]

    /**
     * Edges that compose a square edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *       (4,10)
     *       # - #
     * (2,8) |   |(5,11)
     *       # - #
     *       (3,9)
     *
     */
    final static List<Integer> EMBEDDING_SQUARE_EDGES = [2, 3, 4, 5, 8, 9, 10, 11]

    /**
     * Edges that compose a edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *          # -(4,10)- #
     *     (2,8)|    (5,11)| \ (7)
     *  # -(1)- # -(3, 9)- # - #
     *       (0)|            (6)
     *          #
     *
     */
    final static List<Integer> EMBEDDING_COMPLETE_EDGES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    @Override
    Map<EmbeddingId, List<Integer>> getEdgeEmbeddingMap() {
        def edgeEmbeddingMap = new HashMap<>(super.getEdgeEmbeddingMap())

        edgeEmbeddingMap.put(EmbeddingId.PATH, EMBEDDING_PATH_EDGES)
        edgeEmbeddingMap.put(EmbeddingId.PATH_ALT, EMBEDDING_PATH_ALT_EDGES)
        edgeEmbeddingMap.put(EmbeddingId.STAR, EMBEDDING_STAR_EDGES)
        edgeEmbeddingMap.put(EmbeddingId.TRIANGLE, EMBEDDING_TRIANGLE_EDGES)
        edgeEmbeddingMap.put(EmbeddingId.SQUARE, EMBEDDING_SQUARE_EDGES)
        edgeEmbeddingMap.put(EmbeddingId.COMPLETE, EMBEDDING_COMPLETE_EDGES)

        return edgeEmbeddingMap
    }
}
