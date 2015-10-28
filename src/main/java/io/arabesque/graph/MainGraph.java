package io.arabesque.graph;

import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.IntIntMap;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface MainGraph<VD extends Writable, QV extends Vertex<VD>, EL extends WritableComparable, QE extends Edge<EL>> {
    void reset();

    boolean isNeighborVertex(int v1, int v2);

    MainGraph<VD, QV, EL, QE> addVertex(QV vertex);

    QV[] getVertices();

    QV getVertex(int vertexId);

    int getNumberVertices();

    QE[] getEdges();

    QE getEdge(int edgeId);

    int getNumberEdges();

    int getEdgeId(int v1, int v2);

    MainGraph<VD, QV, EL, QE> addEdge(QE edge);

    boolean areEdgesNeighbors(int edge1Id, int edge2Id);

    @Deprecated
    boolean isNeighborEdge(int src1, int dest1, int edge2);

    IntIntMap getVertexNeighbourhood(int vertexId);

    IntCollection getVertexNeighbours(int vertexId);
}
