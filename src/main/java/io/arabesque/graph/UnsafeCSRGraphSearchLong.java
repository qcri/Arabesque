package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntIterator;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.function.IntConsumer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.StringTokenizer;

/**
 * Graph used only for Search and thus optimized for search & navigation.
 * Only functionality supported is following labels etc.
 *
 * This graph has only vertex labels.
 *
 * Assume that we have a small number of labels (not thousands).
 * Each vertex has a vector of all labels that point to the neighbors labels.
 * The neighbors of a vertex
 *
 * Assumption: Neighbors are ordered by their LABEL. Need sanity check for this.
 *             Neighbors with same label are also ordered.
 * Assumption: Vertex Labels are from 0..NumLabels.
 * Assumption: The graph has both directions if it is undirected.
 * Created by siganos on 4/20/16.
 */
public class UnsafeCSRGraphSearchLong extends UnsafeCSRMainGraphLong
        implements SearchGraph{

    // The distribution of labels on vertex, used for search based graphs.
    protected HashIntIntMap               reverseVertexlabelCount;
    protected HashIntObjMap<IntArrayList> reverseVertexlabel; //TODO convert to UNSAFE.
    protected long                        vertexNeighLabelPos;
    protected int                         numLabels;

    public UnsafeCSRGraphSearchLong(String name) {
        super(name);
    }

    public UnsafeCSRGraphSearchLong(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public UnsafeCSRGraphSearchLong(Path filePath) throws IOException {
        super(filePath);
    }

    public UnsafeCSRGraphSearchLong(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    private void setVertexNeighborLabelPos(int index, int index2, long value){
        if (index>numVertices || index2 > numLabels || index < 0 || index2 <0){
            throw new RuntimeException("Accessing above the limits:"+index+ " "+index2);
        }
        UNSAFE.putLong(vertexNeighLabelPos + (index * numLabels * LONG_SIZE_IN_BYTES +
                                                index2 * LONG_SIZE_IN_BYTES), value);
    }

    private long getVertexNeighborLabelPos(int index, int index2) {
        if (index>numVertices || index2 > numLabels || index < 0 || index2 <0){
            throw new RuntimeException("Accessing above the limits:"+index+ " "+index2);
        }

        return UNSAFE.getLong(vertexNeighLabelPos + (index * numLabels * LONG_SIZE_IN_BYTES +
                                                    index2 * LONG_SIZE_IN_BYTES));
    }

    @Override
    protected void build() {
        super.build();
        reverseVertexlabelCount = HashIntIntMaps.newMutableMap();
        reverseVertexlabel      = HashIntObjMaps.newMutableMap();

        numLabels = (int) Configuration.get().getNumberLabels();

        if (numLabels<0){
            throw new RuntimeException("Need to know in advance the number of labels");
        }

        //TODO: implement an alternative class.
        if (numLabels>1000){
            throw new RuntimeException("Shouldn't use this class, way too inefficient(Reminder)");
        }

    }

    @Override
    /**
     * Adding the reverse label only.
     */
    protected int parse_vertex(StringTokenizer tokenizer, int prev_vertex_id,
                                long edges_position) {
        int vertexId = Integer.parseInt(tokenizer.nextToken());
        int vertexLabel = Integer.parseInt(tokenizer.nextToken());
        if (prev_vertex_id + 1 != vertexId) {
            throw new RuntimeException("Input graph isn't sorted by vertex id, or vertex id not sequential\n " +
                "Expecting:" + (prev_vertex_id + 1) + " Found:" + vertexId);
        }

        setVertexPos(vertexId,edges_position);
        setVertexLabel(vertexId,vertexLabel);
        reverseVertexlabelCount.addValue(vertexLabel,1,0);
        IntArrayList list = reverseVertexlabel.get(vertexLabel);
        if (list == null){
            list = new IntArrayList(1024);
            reverseVertexlabel.put(vertexLabel,list);
        }
        list.add(vertexId);

        return vertexId;
    }

    @Override
    /**
     * We need a special graph format for the UNSAFE to work properly.
     * The graph is sorted by the label.
     */
    protected long parse_edge(StringTokenizer tokenizer, int vertexId,
                             long edges_position) {
        while (tokenizer.hasMoreTokens()) {
            int neighborId = Integer.parseInt(tokenizer.nextToken());
            setEdgeSource(edges_position, vertexId);
            setEdgeDst(edges_position, neighborId);
            edges_position++;
        }
        return edges_position;
    }

    @Override
    /**
     * Convert the Map of labels to Unsafe memory.
     */
    void end_reading() {
        super.end_reading();
        //System.out.println("Start building now next reference!!!");
        vertexNeighLabelPos = UNSAFE.allocateMemory((numVertices*numLabels+1L) * LONG_SIZE_IN_BYTES);
        long neigh=0;
        for (int i=0;i<numVertices;i++){
            //System.out.println("\t doing: "+i);
            neigh         = getVertexPos(i);
            long neigh_end = getVertexPos(i+1);

            int prevLabel = 0;
            //boolean has_neighbors = false;
            while (neigh < neigh_end) {
              //  has_neighbors = true;
                int label = getVertexLabel(getEdgeDst(neigh));
                //System.out.println(i+" -> "+getEdgeDst(neigh)+ "[ "+label+"]"+ "\t"+prevLabel);

                if (prevLabel != label && label > 0){
                    while ( label > prevLabel){
                        //Do we have missing?
                        //System.out.println("\t\tAdding[missing]:"+i+" "+prevLabel+" " +neigh);
                        setVertexNeighborLabelPos(i, prevLabel, neigh);
                        prevLabel++;
                    }
                }
                prevLabel = label;
                neigh++;
            }
            //System.out.println("\t\tMISSING Adding:"+i+" "+prevLabel+" " +neigh);

            setVertexNeighborLabelPos(i, prevLabel, neigh);
            prevLabel++;
            //Add the missing one, if any.
            while (prevLabel < numLabels) {
                //Do we have missing?
                //System.out.println("\t\tMISSING: Adding "+i+" "+prevLabel+" " +neigh);
                setVertexNeighborLabelPos(i, prevLabel, neigh);
                prevLabel++;
            }
        }
        //Add the last so that we don't care about limits.
        setVertexNeighborLabelPos((int)numVertices,0,neigh);
    }

    private long getVertexNeighborLabelStart(int i, int label){
        if (i == 0 & label == 0) {
            return 0;
        }

        if (label == 0){
            return getVertexNeighborLabelPos(i-1, numLabels-1);
        }

        return getVertexNeighborLabelPos(i,label-1);
    }

    private long getVertexNeighborLabelEnd(int i, int label){
        return getVertexNeighborLabelPos(i, label);
    }

    @Override
    public int getNeighborhoodSizeWithLabel(int i, int label) {
        //System.out.println("Input:"+i+" label:"+
        //    label+"     output:"+getVertexNeighborLabelEnd(i,label)+ "  "+getVertexNeighborLabelStart(i,label));
        if (label >=0) {
            return (int) (getVertexNeighborLabelEnd(i, label) - getVertexNeighborLabelStart(i, label));
        }
        //Match all labels.
        return (int) (getVertexNeighborLabelEnd(i, numLabels-1) - getVertexNeighborLabelStart(i, 0));
    }

    @Override
    public long getNumberVerticesWithLabel(int label) {
        int numVerticesWithLabel = reverseVertexlabelCount.getOrDefault(label,-1);
        if (numVerticesWithLabel == -1){
            return numVertices;
        } else {
            return numVerticesWithLabel;
        }
    }

    @Override
    public boolean hasEdgesWithLabels(int source, int destination, IntArrayList edgeLabels) {
        //We have a special label also -1, keep in mind about this.
        throw new RuntimeException("Shouldn't be called");
    }

    @Override
    public void setIteratorForNeighborsWithLabel(int vertexId, int vertexLabel, IntIterator _one) {
        MyIterator one = (MyIterator) _one;
        one.setVertexLabel(vertexId,vertexLabel);
    }

    @Override
    public IntArrayList getVerticesWithLabel(int vertexLabel) {
        return reverseVertexlabel.get(vertexLabel);
    }

    @Override
    @Deprecated
    public void forEachEdgeId(int v1, int v2, IntConsumer intConsumer) {
        throw new RuntimeException("Shouldn't be used for Search");
    }

    @Override
    @Deprecated
    /**
     * Neighbors are sorted by label, so we should take into account this.
     */
    public boolean isNeighborVertex(int v1, int v2) {
        throw new RuntimeException("Shouldn't be used for Search");
    }

    /**
     * Should be one per thread accessing the graph.
     *
     * @return
     */
    @Override
    public synchronized IntIterator createNeighborhoodSearchIterator() {
        return new MyIterator(this);
    }

    @Override
    public boolean isNeighborVertexWithLabel(int sourceVertexId, int destVertexId, int destinationLabel) {
        //System.out.println("\t\t Checking:"+sourceVertexId+"  "+destVertexId+"   "+destinationLabel);
        long start;
        long end;

        // First check if destination has this label.
        if (numLabels > 1){

            if (destinationLabel != getVertexLabel(destVertexId)){
                //Doesn't matter if they connect they don't have the same label.
                return false;
            }
        }

        if (destinationLabel >= 0){
            //Matches any labels, so we just need to have a common neighbor.
            start = getVertexNeighborLabelStart(sourceVertexId,destinationLabel);
            end = getVertexNeighborLabelEnd(sourceVertexId,destinationLabel);
            if (start < 0 || end > numEdges){
                throw new RuntimeException("Accessing above the limits:"+start+"  "+end);
            }
            final long key = binarySearch0l(edgesIndex, start, end, destVertexId);
            return key >= 0;
        }

        // If label is negative is a special label that matches all the labels.
        // No binary search is possible across labels. Only within a label.
        for (int i = 0; i<numLabels; i++){
            start = getVertexNeighborLabelStart(sourceVertexId,i);
            end = getVertexNeighborLabelEnd(sourceVertexId,i);

            if (start < 0 || end > numEdges){
                throw new RuntimeException("Accessing above the limits:"+start+"  "+end);
            }

            final long key = binarySearch0l(edgesIndex, start, end, destVertexId);
            if(key >= 0){
                return true;
            }
        }
        return false;
    }

    public class MyIterator implements IntIterator {
        private UnsafeCSRGraphSearchLong graph;
        private long pos;
        private long end;

        private MyIterator(UnsafeCSRGraphSearchLong graph) {
            this.graph = graph;
        }


        private void setVertexLabel(int vertexId, int vertexLabel){
            if (vertexLabel>=0) {
                pos = graph.getVertexNeighborLabelStart(vertexId, vertexLabel);
                end = graph.getVertexNeighborLabelEnd(vertexId, vertexLabel);
            }
            else{
                //Special label matches all the labels.
                pos = graph.getVertexNeighborLabelStart(vertexId, 0);
                end = graph.getVertexNeighborLabelEnd(vertexId, numLabels-1);

            }
            //System.out.println("Vertex_id:"+vertexId+ "   Label:"+ vertexLabel+ "Pos:"+pos+ "   to:"+end);
        }

        @Override
        public int nextInt() {
            return graph.getEdgeDst(pos++);
        }

        @Override
        public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public boolean hasNext() {
           // System.out.println("\t"+pos+"  -> "+end);
            return end > pos;
        }

        @Override
        public Integer next() {
            throw new RuntimeException("Shouldn't be used");
        }

        @Override
        public void remove() {
            throw new RuntimeException("Shouldn't be used");
        }
    }
}