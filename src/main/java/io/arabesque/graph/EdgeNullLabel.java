package io.arabesque.graph;

import org.apache.hadoop.io.NullWritable;

public class EdgeNullLabel extends Edge<NullWritable> {
    public EdgeNullLabel() {
    }

    public EdgeNullLabel(int srcId, int destId) {
        super(srcId, destId, NullWritable.get());
    }

    public EdgeNullLabel(int edgeId, int srcId, int destId) {
        super(edgeId, srcId, destId, NullWritable.get());
    }

    @Override
    protected NullWritable createEdgeLabel() {
        return NullWritable.get();
    }
}
