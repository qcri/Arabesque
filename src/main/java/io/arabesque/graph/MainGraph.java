package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;

public interface MainGraph {
    void reset();

    boolean isNeighborVertex(int v1, int v2);

    MainGraph addVertex(Vertex vertex);

    Vertex[] getVertices();

    Vertex getVertex(int vertexId);

    int getNumberVertices();

    Edge[] getEdges();

    Edge getEdge(int edgeId);

    int getNumberEdges();

    ReclaimableIntCollection getEdgeIds(int v1, int v2);

    MainGraph addEdge(Edge edge);

    boolean areEdgesNeighbors(int edge1Id, int edge2Id);

    @Deprecated
    boolean isNeighborEdge(int src1, int dest1, int edge2);

    VertexNeighbourhood getVertexNeighbourhood(int vertexId);

    IntCollection getVertexNeighbours(int vertexId);

    boolean isEdgeLabelled();

    boolean isMultiGraph();

    void forEachEdgeId(int v1, int v2, IntConsumer intConsumer);

    //***** QFrag
    int getVertexLabel(int v);
    int getEdgeLabel(int edgeId);
    int getEdgeSource(int edgeId);
    int getEdgeDst(int edgeId);
    int neighborhoodSize(int vertexId);
    void processEdgeNeighbors(int vertexId,IntConsumer intAddConsumer);
    void processVertexNeighbors(int vertexId,IntConsumer intAddConsumer);

    //***** End of QFrag
}
