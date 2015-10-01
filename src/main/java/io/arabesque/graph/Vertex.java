package io.arabesque.graph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Vertex<VD extends Writable> implements Writable {

    private int vertexId;
    private int vertexLabel;
    private VD vertexData;

    public Vertex() {
        this(0, 0);
    }

    public Vertex(int vertexId, int vertexLabel) {
        this(vertexId, vertexLabel, null);
        vertexData = createVertexData();
    }

    public Vertex(int vertexId, int vertexLabel, VD vertexData) {
        this.vertexId = vertexId;
        this.vertexLabel = vertexLabel;
        this.vertexData = vertexData;
    }

    public int getVertexId() {
        return vertexId;
    }

    public int getVertexLabel() {
        return vertexLabel;
    }

    public VD getVertexData() {
        return vertexData;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.vertexId);
        dataOutput.writeInt(this.vertexLabel);
        vertexData.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vertexId = dataInput.readInt();
        this.vertexLabel = dataInput.readInt();
        vertexData.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int result = vertexId;
        result = 31 * vertexLabel;
        result = 31 * result + vertexData.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vertex qanatEdge = (Vertex) o;

        if (vertexId != qanatEdge.vertexId) return false;
        if (vertexLabel != qanatEdge.vertexLabel) return false;
        if (!vertexData.equals(qanatEdge.vertexData)) return false;

        return true;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "vertexId=" + vertexId +
                "vertexLabel=" + vertexLabel +
                ", vertexData=" + vertexData.toString() +
                '}';
    }

    protected abstract VD createVertexData();

    public void setVertexLabel(int vertexLabel) {
        this.vertexLabel = vertexLabel;
    }
}
