package io.arabesque.graph;

import java.io.IOException;
import java.nio.file.Path;
import java.util.StringTokenizer;

/**
 * Created by siganos on 3/15/16.
 */
public class UnsafeCSRMainGraphEdgeLabelFloat extends UnsafeCSRMainGraph{

    private long labelIndex;
    final static long FLOAT_SIZE_IN_BYTES = 4;

    public UnsafeCSRMainGraphEdgeLabelFloat(String name) {
        super(name);
    }

    public UnsafeCSRMainGraphEdgeLabelFloat(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public UnsafeCSRMainGraphEdgeLabelFloat(Path filePath) throws IOException {
        super(filePath);
    }

    public UnsafeCSRMainGraphEdgeLabelFloat(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    @Override
    protected void build() {
        super.build();
        //System.out.println("Building the Unsafe Edge label float");
        labelIndex = UNSAFE.allocateMemory(numEdges * FLOAT_SIZE_IN_BYTES);
    }

    @Override
    public int getEdgeLabel(int index) {
        throw new RuntimeException("Can not use this graph because it has float labels.");
    }

    public void setEdgeLabel(int index, float value) {
        if (index>numEdges){
            throw new RuntimeException("Exceeding limit for writing index"+index);
        }
        UNSAFE.putFloat(labelIndex+(index*INT_SIZE_IN_BYTES),value);
    }

    public float getEdgeLabelFloat(int index) {
        return UNSAFE.getFloat(labelIndex+(index*FLOAT_SIZE_IN_BYTES));
    }

    @Override
    public boolean isEdgeLabelled() {
        return true;
    }

    @Override
    protected int parse_edge(StringTokenizer tokenizer, int vertexId, int edges_position) {
        int prev_edge = -1;
        while (tokenizer.hasMoreTokens()) {
            int neighborId = Integer.parseInt(tokenizer.nextToken());

            if (prev_edge >= 0 && prev_edge > neighborId) {
                throw new RuntimeException("The edges need to be sorted for unsafe");
            }

            float edgeLabel = Float.parseFloat(tokenizer.nextToken());
            prev_edge = neighborId;

            if (vertexId<=neighborId) {
                setEdgeSource(edges_position, vertexId);
                setEdgeDst(edges_position, neighborId);
                setEdgeLabel(edges_position, edgeLabel);
                edges_position++;
            }
        }

        return edges_position;
    }

}
