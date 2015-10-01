package io.arabesque.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Edge<
        EL extends WritableComparable>
        implements Writable {
    public static final int DEFAULT_EDGE_ID = -1;

    private int edgeId;
    private int sourceVertexId;
    private int destinationVertexId;
    private EL edgeLabel;

    public Edge() {
        this(DEFAULT_EDGE_ID, -1, -1, null);
    }

    public Edge(int sourceVertexId, int destinationVertexId, EL edgeValue) {
        this(DEFAULT_EDGE_ID, sourceVertexId, destinationVertexId, edgeValue);
    }

    public Edge(int edgeId, int sourceVertexId, int destinationVertexId, EL edgeValue) {
        this.edgeId = edgeId;
        this.sourceVertexId = sourceVertexId;
        this.destinationVertexId = destinationVertexId;

        if (edgeValue != null) {
            this.edgeLabel = edgeValue;
        } else {
            this.edgeLabel = createEdgeLabel();
        }
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

    public EL getEdgeLabel() {
        return edgeLabel;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.edgeId);
        dataOutput.writeInt(this.sourceVertexId);
        dataOutput.writeInt(this.destinationVertexId);
        dataOutput.writeBoolean(true);
        edgeLabel.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.edgeId = dataInput.readInt();
        this.sourceVertexId = dataInput.readInt();
        this.destinationVertexId = dataInput.readInt();
        edgeLabel.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return edgeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Edge edge = (Edge) o;

        if (edgeId != edge.edgeId) return false;
        if (destinationVertexId != edge.destinationVertexId) return false;
        if (sourceVertexId != edge.sourceVertexId) return false;
        if (!edgeLabel.equals(edge.edgeLabel)) return false;

        return true;
    }

    @Override
    public String toString() {
        return "Edge{" +
                "edgeId=" + edgeId +
                "sourceVertexId=" + sourceVertexId +
                ", destinationVertexId=" + destinationVertexId +
                ", edgeLabel=" + edgeLabel +
                '}';
    }

    public boolean hasVertex(int vertexId) {
        if (sourceVertexId == vertexId || destinationVertexId == vertexId) {
            return true;
        }

        return false;
    }

    protected abstract EL createEdgeLabel();

    public boolean neighborWith(Edge<EL> edge2) {
        int src2 = edge2.getSourceId();
        int dst2 = edge2.getDestinationId();

        return hasVertex(src2) || hasVertex(dst2);
    }
}
