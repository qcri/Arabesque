package io.arabesque.testutils.graphs

import io.arabesque.graph.BasicMainGraph
import io.arabesque.pattern.VertexPositionEquivalences

/**
 * Base structure of a graph with 1 triangle, 1 square and a 4-pronged star.
 *
 * Graphical representation:
 *
 *     # - #
 *     |   | \
 * # - # - # - #
 *     |
 *     #
 */
abstract class TestGraph extends BasicMainGraph {
    TestGraph(String name) {
        this(name, false, false);
    }

    TestGraph(String name, boolean edgeLabelled, boolean multiGraph) {
        super(name, edgeLabelled, multiGraph);
    }

    List<List<Integer>> getEmbeddingsVertices() {
        return getVertexEmbeddingMap().values().asList()
    }

    abstract Map<List<Integer>, VertexPositionEquivalences> getVEquivMap();

    final static List<Integer> EMBEDDING_EMPTY = []
    final static List<Integer> EMBEDDING_SINGLE_VERTICES = [1]
    final static List<Integer> EMBEDDING_SINGLE_EDGES = [1]

    /**
     * Vertices that compose a path vertex induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     * # - # - # - #
     */
    final static List<Integer> EMBEDDING_PATH_VERTICES = [2, 0, 4, 6]

    /**
     * Edges that compose a path edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     * # - # - # - #
     */
    final static List<Integer> EMBEDDING_PATH_EDGES = [1, 3, 6]

    /**
     * Vertices that compose an alternative path vertex induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     # - #
     *     |
     *     #
     *     |
     *     #
     *
     */
    final static List<Integer> EMBEDDING_PATH_ALT_VERTICES = [0, 1, 3, 5]

    /**
     * Edges that compose an alternative path edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     # - #
     *     |
     *     #
     *     |
     *     #
     *
     */
    final static List<Integer> EMBEDDING_PATH_ALT_EDGES = [0, 2, 4]

    /**
     * Vertices that compose a star vertex induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     #
     *     |
     * # - # - #
     *     |
     *     #
     */
    final static List<Integer> EMBEDDING_STAR_VERTICES = [0, 1, 2, 3, 4]

    /**
     * Edges that compose a star edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     #
     *     |
     * # - # - #
     *     |
     *     #
     *
     */
    final static List<Integer> EMBEDDING_STAR_EDGES = [0, 1, 2, 3]

    /**
     * Vertices that compose a triangle vertex induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *         #
     *         | \
     *         # - #
     */
    final static List<Integer> EMBEDDING_TRIANGLE_VERTICES = [4, 5, 6]

    /**
     * Edges that compose a triangle edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *         #
     *         | \
     *         # - #
     *
     */
    final static List<Integer> EMBEDDING_TRIANGLE_EDGES = [5, 6, 7]

    /**
     * Vertices that compose a square vertex induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     # - #
     *     |   |
     *     # - #
     */
    final static List<Integer> EMBEDDING_SQUARE_VERTICES = [0, 3, 4, 5]

    /**
     * Edges that compose a square edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     # - #
     *     |   |
     *     # - #
     *
     */
    final static List<Integer> EMBEDDING_SQUARE_EDGES = [2, 3, 4, 5]

    /**
     * Vertices that compose a vertex induced embedding over the entire test graphs.
     *
     * Graphical representation:
     *
     *     # - #
     *     |   | \
     * # - # - # - #
     *     |
     *     #
     */
    final static List<Integer> EMBEDDING_COMPLETE_VERTICES = [0, 1, 2, 3, 4, 5, 6]

    /**
     * Edges that compose a edge induced embedding over the test graphs.
     *
     * Graphical representation:
     *
     *     # - #
     *     |   | \
     * # - # - # - #
     *     |
     *     #
     *
     */
    final static List<Integer> EMBEDDING_COMPLETE_EDGES = [0, 1, 2, 3, 4, 5, 6, 7]

    final static enum EmbeddingId {EMPTY, SINGLE_VERTEX, SINGLE_EDGE, PATH, PATH_ALT, STAR, TRIANGLE, SQUARE, COMPLETE}

    final static Map<EmbeddingId, List<Integer>> VERTEX_EMBEDDINGS_MAP = [
            (EmbeddingId.EMPTY)        : EMBEDDING_EMPTY,
            (EmbeddingId.SINGLE_VERTEX): EMBEDDING_SINGLE_VERTICES,
            (EmbeddingId.PATH)         :  EMBEDDING_PATH_VERTICES,
            (EmbeddingId.PATH_ALT)     : EMBEDDING_PATH_ALT_VERTICES,
            (EmbeddingId.STAR)         : EMBEDDING_STAR_VERTICES,
            (EmbeddingId.TRIANGLE)     : EMBEDDING_TRIANGLE_VERTICES,
            (EmbeddingId.SQUARE)       : EMBEDDING_SQUARE_VERTICES,
            (EmbeddingId.COMPLETE)     : EMBEDDING_COMPLETE_VERTICES,
    ]

    Map<EmbeddingId, List<Integer>> getVertexEmbeddingMap() {
        return VERTEX_EMBEDDINGS_MAP
    }

    final static Map<EmbeddingId, List<Integer>> EDGE_EMBEDDINGS_MAP = [
            (EmbeddingId.EMPTY)      : EMBEDDING_EMPTY,
            (EmbeddingId.SINGLE_EDGE): EMBEDDING_SINGLE_EDGES,
            (EmbeddingId.PATH)       :  EMBEDDING_PATH_EDGES,
            (EmbeddingId.PATH_ALT)   : EMBEDDING_PATH_ALT_EDGES,
            (EmbeddingId.STAR)       : EMBEDDING_STAR_EDGES,
            (EmbeddingId.TRIANGLE)   : EMBEDDING_TRIANGLE_EDGES,
            (EmbeddingId.SQUARE)     : EMBEDDING_SQUARE_EDGES,
            (EmbeddingId.COMPLETE)   : EMBEDDING_COMPLETE_EDGES,
    ]

    Map<EmbeddingId, List<Integer>> getEdgeEmbeddingMap() {
        return EDGE_EMBEDDINGS_MAP
    }
}
