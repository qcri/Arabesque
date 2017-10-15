package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

import java.io.IOException;
import java.nio.file.Path;
import java.util.StringTokenizer;

/**
 * Created by siganos on 3/21/16.
 */
public class UnsafeCSRGraphSearchEdgeLabelLong
                    extends UnsafeCSRGraphSearchLong {

    private long vertexEdgeLabelPos; // indexing for edge labels per vertex....
    private long edgeLabelSearchDst; // The neighbour that has some label that is found through the above pos.
    private int  numEdgeLabels;

    public UnsafeCSRGraphSearchEdgeLabelLong(String name) {
        super(name);
    }

    public UnsafeCSRGraphSearchEdgeLabelLong(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public UnsafeCSRGraphSearchEdgeLabelLong(Path filePath) throws IOException {
        super(filePath);
    }

    public UnsafeCSRGraphSearchEdgeLabelLong(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    @Override
    public void build() {
        super.build();
        numEdgeLabels      = Configuration.get().getNumberEdgesLabels();
        if (numEdgeLabels < 0){
            throw new RuntimeException("Number of edge labels required");
        }
        vertexEdgeLabelPos = UNSAFE.allocateMemory(numVertices * numEdgeLabels * LONG_SIZE_IN_BYTES);
        edgeLabelSearchDst    = UNSAFE.allocateMemory(numEdges * INT_SIZE_IN_BYTES);
    }

    public void setEdgeLabelDstSearch(long index, int value) {
        if (index > numEdges || index < 0){
            throw new RuntimeException("Accessing above limits(numEdges):"+index);
        }
        UNSAFE.putInt(edgeLabelSearchDst + (index * INT_SIZE_IN_BYTES), value);
    }

    public void setVertexEdgeLabelPos(int index, int index2, long value) {
        if (index > numVertices || index2 > numEdgeLabels || index < 0 || index2 <0){
            throw new RuntimeException("Accessing above limits:"+index+"  or"+index2);
        }
        UNSAFE.putLong(vertexEdgeLabelPos + (index * numEdgeLabels * LONG_SIZE_IN_BYTES +
                                            index2 * LONG_SIZE_IN_BYTES), value);
    }

    public int getEdgeLabelDstSearch(int index) {
        return UNSAFE.getInt(edgeLabelSearchDst+(index*INT_SIZE_IN_BYTES));
    }

    public long getVertexEdgeLabelPos(int index, int index2) {
        if (index > numVertices || index2 > numEdgeLabels || index < 0 || index2 <0){
            throw new RuntimeException("Accessing above limits:"+index+"  or"+index2);
        }

        return UNSAFE.getLong(vertexEdgeLabelPos + (index * numEdgeLabels * LONG_SIZE_IN_BYTES +
            index2 * LONG_SIZE_IN_BYTES));
    }

    @Override
    protected long parse_edge(StringTokenizer tokenizer, int vertexId, long edges_position) {
        HashIntObjMap<IntArrayList> _tmp = HashIntObjMaps.newMutableMap();
        long start = edges_position;

        while (tokenizer.hasMoreTokens()) {
            int neighborId = Integer.parseInt(tokenizer.nextToken());
            setEdgeSource(edges_position, vertexId);
            setEdgeDst(edges_position, neighborId);

            // Next we deal with the edge label.
            int edgeLabel = Integer.parseInt(tokenizer.nextToken());

            //setEdgeLabel(edges_position,edgeLabel);

            IntArrayList verticesWithLabel = _tmp.get(edgeLabel);
            if (verticesWithLabel == null) {
                verticesWithLabel = new IntArrayList();
                _tmp.put(edgeLabel, verticesWithLabel);
            }
            verticesWithLabel.add(neighborId); // Assuming edge labels.
            edges_position++;

        }

        //System.out.println("Doing vertex:"+vertexId);
        // Next, we sort based on edge label, and sort per label neighbor.
        for (int i = 0; i < numEdgeLabels; i++){
            //System.out.println("\tLabel:"+i);
            IntArrayList res = _tmp.get(i);
            setVertexEdgeLabelPos(vertexId,i,start);

            if (res==null){
                //Missing label.
                continue;
            }
            res.sort();
            for (int j = 0; j < res.size(); j++){
                //System.out.println("\t\t->"+start+" -> "+res.getUnchecked(j));
                setEdgeLabelDstSearch(start,res.getUnchecked(j));
                start++;
            }
        }
        return edges_position;
    }

    @Override
    void end_reading() {
        super.end_reading();

        // Make sure we fix the edge label position by adding an extra fake vertex with 1 label for the pos.
        long lastPos = getVertexEdgeLabelPos((int) (numVertices - 1), numEdgeLabels - 1);
        long endPos;
        if (numEdgeLabels>1){
            endPos = getVertexEdgeLabelPos((int) (numVertices-1),numEdgeLabels-2);
        }
        else{
            endPos = getVertexEdgeLabelPos((int)(numVertices-2),numEdgeLabels-1);
        }

        if (lastPos==endPos){
            setVertexEdgeLabelPos((int) numVertices,0,endPos); // will be missing...
        }
        else{
            setVertexEdgeLabelPos((int)numVertices,0,lastPos+1);
        }
    }

    @Override
    public boolean hasEdgesWithLabels(int source, int destination, IntArrayList edgeLabels) {

        for (int i = 0;i<edgeLabels.size();i++){
            boolean result = hasEdgesWithLabel(source, destination, edgeLabels.getUnchecked(i));
            if (!result){
                return false;
            }
        }
        //all matched.
        return true;
    }

    private boolean hasEdgesWithLabel(int source, int destination, int label) {
        long start;
        long end;
        //System.out.println(source+" "+destination+" "+label);
        if (label >=0){
            start = getVertexNeighborEdgeLabelStart(source,label);
            end = getVertexNeighborEdgeLabelEnd(source,label);
            if (start < 0 || end > numEdges){
                throw new RuntimeException("Accessing above the limits:"+start+"  "+end);
            }

            final long key = binarySearch0l(edgeLabelSearchDst, start, end, destination);
            return key>=0;
        }

        // Special label matches all, we simply want to know if we connect with this neighbor.
        for (int i = 0; i < numEdgeLabels; i++){
            start = getVertexNeighborEdgeLabelStart(source,i);
            end = getVertexNeighborEdgeLabelEnd(source,i);

            if (start < 0 || end > numEdges){
                throw new RuntimeException("Accessing above the limits:"+start+"  "+end);
            }
            final long key = binarySearch0l(edgeLabelSearchDst, start, end, destination);
            if (key>=0){
                return true;
            }
        }
        return false;
    }

    private long getVertexNeighborEdgeLabelStart(int i, int label){
        return getVertexEdgeLabelPos(i,label);
    }

    private long getVertexNeighborEdgeLabelEnd(int i, int label){
        if (label == (numEdgeLabels-1)){
            return getVertexEdgeLabelPos(i+1,0);
        }
        return getVertexEdgeLabelPos(i, label+1);
    }


}
