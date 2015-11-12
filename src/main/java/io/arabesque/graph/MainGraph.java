package io.arabesque.graph;

import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.IntIntMap;

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

    int getEdgeId(int v1, int v2);

    MainGraph addEdge(Edge edge);

    boolean areEdgesNeighbors(int edge1Id, int edge2Id);

    @Deprecated
    boolean isNeighborEdge(int src1, int dest1, int edge2);

    IntIntMap getVertexNeighbourhood(int vertexId);

    IntCollection getVertexNeighbours(int vertexId);
}
