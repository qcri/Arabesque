package io.arabesque.graph;

import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.StringTokenizer;

public class NullDataMainGraph extends BasicMainGraph<
        NullWritable,
        VertexIntLabelNullData,
        NullWritable,
        EdgeNullLabel> {

    public NullDataMainGraph(Path filePath) throws IOException {
        super(filePath);
    }

    public NullDataMainGraph(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    @Override
    protected NullWritable readVertexData(StringTokenizer tokenizer) {
        return NullWritable.get();
    }

    @Override
    protected VertexIntLabelNullData createVertex(int id, int label, NullWritable data) {
        return new VertexIntLabelNullData(id, label);
    }

    @Override
    protected VertexIntLabelNullData createVertex() {
        return new VertexIntLabelNullData();
    }

    @Override
    protected EdgeNullLabel createEdge(int srcId, int destId) {
        return new EdgeNullLabel(srcId, destId);
    }

    @Override
    protected EdgeNullLabel createEdge() {
        return new EdgeNullLabel();
    }
}
