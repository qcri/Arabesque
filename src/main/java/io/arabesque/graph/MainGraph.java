package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;

public interface MainGraph {
    void reset();

    int getVertexLabel(int v);

    boolean isNeighborVertex(int v1, int v2);


    int getNumberVertices();
    int getNumberEdges();

    int getEdgeLabel(int edgeId);
    int getEdgeSource(int edgeId);
    int getEdgeDst(int edgeId);

    int neighborhoodSize(int vertexId);

//    ReclaimableIntCollection getEdgeIds(int v1, int v2);


    boolean areEdgesNeighbors(int edge1Id, int edge2Id);


    void processEdgeNeighbors(int vertexId,IntConsumer intAddConsumer);
    void processVertexNeighbors(int vertexId,IntConsumer intAddConsumer);


    boolean isEdgeLabelled();

    boolean isMultiGraph();

    @Deprecated
    // This shouldn't be used. Inefficient design typically.
    void forEachEdgeId(int v1, int v2, IntConsumer intConsumer);

    MainGraph addEdge(Edge edge);
    MainGraph addVertex(Vertex vertex);

}
