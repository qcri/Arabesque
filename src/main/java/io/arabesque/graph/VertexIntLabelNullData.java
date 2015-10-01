package io.arabesque.graph;

import org.apache.hadoop.io.NullWritable;

public class VertexIntLabelNullData extends Vertex<NullWritable> {
    public VertexIntLabelNullData() {
    }

    public VertexIntLabelNullData(int vertexId, int vertexLabel) {
        super(vertexId, vertexLabel);
    }

    @Override
    protected NullWritable createVertexData() {
        return NullWritable.get();
    }
}
