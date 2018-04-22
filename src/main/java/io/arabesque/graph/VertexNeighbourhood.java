package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public interface VertexNeighbourhood {
    IntCollection getNeighbourVertices();
    IntCollection getNeighbourEdges();
    ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId);

    boolean isNeighbourVertex(int vertexId);

    void addEdge(int neighbourVertexId, int edgeId);

    void forEachEdgeId(int nId, IntConsumer intConsumer);

    void write (ObjectOutput out) throws IOException;
    void read (ObjectInput in) throws IOException;
}
