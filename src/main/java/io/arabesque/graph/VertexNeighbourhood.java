package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import net.openhft.koloboke.collect.IntCollection;

public interface VertexNeighbourhood {
    IntCollection getNeighbourVertices();
    IntCollection getNeighbourEdges();
    ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId);

    boolean isNeighbourVertex(int vertexId);

    void addEdge(int neighbourVertexId, int edgeId);
}
