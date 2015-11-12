package io.arabesque.graph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Edge implements Writable {
    public static final int DEFAULT_EDGE_ID = -1;

    private int edgeId;
    private int sourceVertexId;
    private int destinationVertexId;

    public Edge() {
        this(DEFAULT_EDGE_ID, -1, -1);
    }

    public Edge(int sourceVertexId, int destinationVertexId) {
        this(DEFAULT_EDGE_ID, sourceVertexId, destinationVertexId);
    }

    public Edge(int edgeId, int sourceVertexId, int destinationVertexId) {
        this.edgeId = edgeId;
        this.sourceVertexId = sourceVertexId;
        this.destinationVertexId = destinationVertexId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    void setEdgeId(int edgeId) {
        this.edgeId = edgeId;
    }

    public int getSourceId() {
        return sourceVertexId;
    }

    public int getDestinationId() {
        return destinationVertexId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.edgeId);
        dataOutput.writeInt(this.sourceVertexId);
        dataOutput.writeInt(this.destinationVertexId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.edgeId = dataInput.readInt();
        this.sourceVertexId = dataInput.readInt();
        this.destinationVertexId = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Edge edge = (Edge) o;

        if (edgeId != edge.edgeId) return false;
        if (sourceVertexId != edge.sourceVertexId) return false;
        return destinationVertexId == edge.destinationVertexId;

    }

    @Override
    public int hashCode() {
        int result = edgeId;
        result = 31 * result + sourceVertexId;
        result = 31 * result + destinationVertexId;
        return result;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "edgeId=" + edgeId +
                "sourceVertexId=" + sourceVertexId +
                ", destinationVertexId=" + destinationVertexId +
                '}';
    }

    public boolean hasVertex(int vertexId) {
        return sourceVertexId == vertexId || destinationVertexId == vertexId;
    }

    public boolean neighborWith(Edge edge2) {
        int src2 = edge2.getSourceId();
        int dst2 = edge2.getDestinationId();

        return hasVertex(src2) || hasVertex(dst2);
    }
}
