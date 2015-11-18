package io.arabesque.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LabelledEdge extends Edge {
    private int edgeLabel;

    public LabelledEdge() {
        edgeLabel = 0;
    }

    public LabelledEdge(int sourceVertexId, int destinationVertexId, int edgeLabel) {
        super(sourceVertexId, destinationVertexId);

        this.edgeLabel = edgeLabel;
    }

    public LabelledEdge(int edgeId, int sourceVertexId, int destinationVertexId, int edgeLabel) {
        super(edgeId, sourceVertexId, destinationVertexId);

        this.edgeLabel = edgeLabel;
    }

    public int getEdgeLabel() {
        return edgeLabel;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeInt(edgeLabel);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        edgeLabel = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LabelledEdge that = (LabelledEdge) o;

        return edgeLabel == that.edgeLabel;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + edgeLabel;
        return result;
    }

    @Override
    public String toString() {
        return "LabelledEdge{" +
                "edgeLabel=" + edgeLabel +
                "} " + super.toString();
    }
}
