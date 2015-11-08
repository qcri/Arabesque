package io.arabesque.graph;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Vertex implements Writable {

    private int vertexId;
    private int vertexLabel;

    public Vertex() {
        this(0, 0);
    }

    public Vertex(int vertexId, int vertexLabel) {
        this.vertexId = vertexId;
        this.vertexLabel = vertexLabel;
    }

    public int getVertexId() {
        return vertexId;
    }

    public int getVertexLabel() {
        return vertexLabel;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.vertexId);
        dataOutput.writeInt(this.vertexLabel);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.vertexId = dataInput.readInt();
        this.vertexLabel = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vertex vertex = (Vertex) o;

        if (vertexId != vertex.vertexId) return false;
        return vertexLabel == vertex.vertexLabel;

    }

    @Override
    public int hashCode() {
        int result = vertexId;
        result = 31 * result + vertexLabel;
        return result;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "vertexId=" + vertexId +
                "vertexLabel=" + vertexLabel +
                '}';
    }
}
