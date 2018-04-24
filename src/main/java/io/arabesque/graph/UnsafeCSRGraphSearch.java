package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.collect.IntIterator;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashIntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.hash.HashObjSet;
import com.koloboke.function.IntConsumer;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.Path;
import java.util.Map.Entry;
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
public class UnsafeCSRGraphSearch extends UnsafeCSRMainGraph
        implements SearchGraph, Externalizable {

    private static final Logger LOG = Logger.getLogger(UnsafeCSRGraphSearch.class);

    // The distribution of labels on vertex, used for search based graphs.
    protected HashIntIntMap               reverseVertexlabelCount;
    protected HashIntObjMap<IntArrayList> reverseVertexlabel; //TODO convert to UNSAFE.
    protected HashIntObjMap<HashIntIntMap> neighborhoodMap; // So that neighbors can be fast.
    protected long                        vertexNeighLabelPos;
    protected int                         numLabels; // no default
    private boolean fastNeighbors = true;

    public UnsafeCSRGraphSearch() {};

    public UnsafeCSRGraphSearch(String name) {
        super(name);
    }

    public UnsafeCSRGraphSearch(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public UnsafeCSRGraphSearch(Path filePath) throws IOException {
        super(filePath);
    }

    public UnsafeCSRGraphSearch(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    void setVertexNeighborLabelPos(int index, int index2, int value){
        if (index>numVertices || index2 >= numLabels || index < 0 || index2 <0){
            throw new RuntimeException("Accessing above the limits (case 1): "+index+ " "+index2);
        }
        if (index == numVertices && index2 > 0){
            throw new RuntimeException("Accessing above the limits (case 2): "+index+ " "+index2);
        }
        UNSAFE.putInt(vertexNeighLabelPos + (index * numLabels * INT_SIZE_IN_BYTES +
                                                index2 * INT_SIZE_IN_BYTES), value);
    }

    private int getVertexNeighborLabelPos(int index, int index2) {
        if (index>numVertices || index2 >= numLabels || index < 0 || index2 <0){
            throw new RuntimeException("Accessing above the limits (case 1):"+index+ " "+index2);
        }
        if (index == numVertices && index2 > 0){
            throw new RuntimeException("Accessing above the limits (case 2):"+index+ " "+index2);
        }

        return UNSAFE.getInt(vertexNeighLabelPos + (index * numLabels * INT_SIZE_IN_BYTES +
                                                    index2 * INT_SIZE_IN_BYTES));
    }

    @Override
    protected void build() {
        super.build();
        Configuration conf = Configuration.get();

        reverseVertexlabelCount = HashIntIntMaps.newMutableMap();
        reverseVertexlabel      = HashIntObjMaps.newMutableMap();

        // DEBUG!!
        // numLabels = (int) Configuration.get().getNumberLabels();
        numLabels = conf.getInteger(conf.SEARCH_NUM_LABELS, conf.SEARCH_NUM_LABELS_DEFAULT);
        // For star
        // numLabels = 1;

        fastNeighbors = Configuration.get().getBoolean(conf.SEARCH_FASTNEIGHBORS, conf.SEARCH_FASTNEIGHBORS_DEFAULT);

        if (fastNeighbors){
            System.out.println("USING MAP FOR NEIGHBORS CHECK");
            neighborhoodMap = HashIntObjMaps.newMutableMap((int) numVertices);
        }

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
    protected int parse_vertex(StringTokenizer tokenizer, int prev_vertex_id, int edges_position) {
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
    protected int parse_edge(StringTokenizer tokenizer, int vertexId,
                             int edges_position) {
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
        if (fastNeighbors){
            fast_end_reading();
        }
        else{
            end_reading_normal();
        }
    }

    private void end_reading_normal(){
        //System.out.println("Start building now next reference!!!");
        vertexNeighLabelPos = UNSAFE.allocateMemory((numVertices*numLabels+1L) * INT_SIZE_IN_BYTES);
        int neigh=0;
        for (int i=0;i<numVertices;i++){
            //System.out.println("\t doing: "+i);
            neigh         = getVertexPos(i);
            int neigh_end = getVertexPos(i+1);



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
    private void fast_end_reading(){
        System.out.println("Start building now next reference!!!");
        vertexNeighLabelPos = UNSAFE.allocateMemory((numVertices*numLabels+1L) * INT_SIZE_IN_BYTES);
        int neigh=0;
        for (int i=0;i<numVertices;i++){
            //System.out.println("\t doing: "+i);
            neigh         = getVertexPos(i);
            int neigh_end = getVertexPos(i+1);


            HashIntIntMap map = HashIntIntMaps.newMutableMap();
            neighborhoodMap.put(i,map);

            int prevLabel = 0;
            //boolean has_neighbors = false;
            while (neigh < neigh_end) {
                //  has_neighbors = true;
                final int neigh_id = getEdgeDst(neigh);
                int label = getVertexLabel(neigh_id);
                //System.out.println(i+" -> "+getEdgeDst(neigh)+ "[ "+label+"]"+ "\t"+prevLabel);
                map.put(neigh_id,label);

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

//            setVertexNeighborLabelPos(i, prevLabel, neigh);
//            prevLabel++;
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

    protected int getVertexNeighborLabelStart(int i, int label){
        if (i == 0 & label == 0) {
            return 0;
        }

        if (label == 0){
            return getVertexNeighborLabelPos(i-1, numLabels-1);
        }

        return getVertexNeighborLabelPos(i,label-1);
    }

    int getVertexNeighborLabelEnd(int i, int label){
        return getVertexNeighborLabelPos(i, label);
    }

    @Override
    public int getNeighborhoodSizeWithLabel(int i, int label) {
        //System.out.println("Input:"+i+" label:"+
        //    label+"     output:"+getVertexNeighborLabelEnd(i,label)+ "  "+getVertexNeighborLabelStart(i,label));
        if (label >=0) {
            return getVertexNeighborLabelEnd(i, label) - getVertexNeighborLabelStart(i, label);
        }
        //Match all labels.
        return getVertexNeighborLabelEnd(i, numLabels-1) - getVertexNeighborLabelStart(i, 0);
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
//    public IntArrayList getVerticesWithLabel(int vertexLabel) {
//        return reverseVertexlabel.get(vertexLabel);
//    }
    public IntArrayList getVerticesWithLabel(int vertexLabel) {
        if (vertexLabel < 0){
            // should not invoke this method if we don't look for a specific label
            return null;
        }
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
        if (fastNeighbors){
            HashIntIntMap map = neighborhoodMap.get(sourceVertexId);
            if (map == null){
                return false;
            }

            int res = map.getOrDefault(destVertexId,-2);
            if (res == -2){
                return false;
            }

            if (destinationLabel >=0){
                return res == destinationLabel;
            }
            //Matches all the destination label, so since neighbors we have a hit.
            return true;
        }
        return isNeighborVertexWithLabel_(sourceVertexId,destVertexId,destinationLabel);
    }

    private boolean isNeighborVertexWithLabel_(int sourceVertexId, int destVertexId, int destinationLabel) {
        //System.out.println("\t\t Checking:"+sourceVertexId+"  "+destVertexId+"   "+destinationLabel);
        int start;
        int end;

        // First check if destination has this label.
        if (numLabels > 1 && destinationLabel>=0){

            if (!vertexHasLabel(destinationLabel, destVertexId)){
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
            final int key = binarySearch0(edgesIndex, start, end, destVertexId);
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

            final int key = binarySearch0(edgesIndex, start, end, destVertexId);
            if(key >= 0){
                return true;
            }
        }
        return false;
    }

    protected boolean vertexHasLabel(int destinationLabel, int destVertexId) {
        return destinationLabel == getVertexLabel(destVertexId);
    }

    public class MyIterator implements IntIterator {
        protected UnsafeCSRGraphSearch graph;
        protected int pos;
        protected int end;

        MyIterator(UnsafeCSRGraphSearch graph) {
            this.graph = graph;
        }


        void setVertexLabel(int vertexId, int vertexLabel){
            if (vertexLabel>=0) {
                pos = graph.getVertexNeighborLabelStart(vertexId, vertexLabel);
                end = graph.getVertexNeighborLabelEnd(vertexId, vertexLabel);
            }
            else{
                //Special label matches all the labels.
                pos = graph.getVertexNeighborLabelStart(vertexId, 0);
                end = graph.getVertexNeighborLabelEnd(vertexId, numLabels-1);

            }
            //System.out.println("\tlabel:"+vertexLabel+" Pos:"+pos+ "   to:"+end);
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

    public void write (ObjectOutput out)
            throws IOException {
        this.writeExternal(out);
    }

    public void read(ObjectInput in) throws IOException, ClassNotFoundException {
        this.readExternal(in);
    }

    public void writeExternal(java.io.ObjectOutput out)
            throws IOException {
        System.out.println("@DEBUG_CONF In UnsafeCSRGraphSearch.writeExternal()");

        // Fields from AbstractMainGraph
        // isMultiGraph is covered by UnsafeCSRMainGraph and set to false in its build method
        out.writeLong(numVertices);
        out.writeLong(numEdges);
        out.writeBoolean(isEdgeLabelled);
        out.writeBoolean(isFloatLabel);
        out.writeBoolean(isBinary);
        out.writeObject(name);

        // Fields from UnsafeCSRMainGraph
        out.writeBoolean(built);

        if (built) {
            for (long index = 0; index <= numVertices; index++) {
                // verticesIndex
                out.writeInt(getVertexPos(index));
                // verticesIndexLabel
                // TODO add a safety check
                // TODO the superclass should take a long parameter, but this would require changing a central interface
                out.writeInt(getVertexLabel((int) index));
            }
            for (long index = 0; index < numEdges; index++) {
                // edgesIndex
                out.writeInt(getEdgeDst((int) index));
                // edgesIndexSource
                out.writeInt(getEdgeSource((int) index));
            }
        }

//        // Fields of this class
        out.writeInt(numLabels);
        out.writeBoolean(fastNeighbors);

        if(reverseVertexlabelCount == null) {
            out.writeInt(-1);
        } else {
            HashObjSet<Entry<Integer, Integer>> entrySet1 = reverseVertexlabelCount.entrySet();
            out.writeInt(entrySet1.size());
            for (Entry<Integer, Integer> entry : entrySet1) {
                out.writeInt(entry.getKey());
                out.writeInt(entry.getValue());
            }
        }

        if(reverseVertexlabel == null){
            out.writeInt(-1);
        } else {
            HashObjSet<Entry<Integer, IntArrayList>> entrySet2 = reverseVertexlabel.entrySet();
            out.writeInt(entrySet2.size());
            for (Entry<Integer, IntArrayList> entry : entrySet2) {
                out.writeInt(entry.getKey());
                IntArrayList list = entry.getValue();
                if (list == null){
                    out.writeInt(-1);
                } else {
                    int size = list.size();
                    out.writeInt(size);
                    for (int i = 0; i < size; i++) {
                        out.writeInt(list.get(i));
                    }
                }
            }
        }

        if (neighborhoodMap == null){
            out.writeInt(-1);
        } else {
            HashObjSet<Entry<Integer, HashIntIntMap>> entrySet3 = neighborhoodMap.entrySet();
            out.writeInt(entrySet3.size());
            for (Entry<Integer, HashIntIntMap> entry : entrySet3) {
                out.writeInt(entry.getKey());
                HashIntIntMap map = entry.getValue();
                if (map == null){
                    out.writeInt(-1);
                } else {
                    HashObjSet<Entry<Integer, Integer>> entrySet4 = map.entrySet();
                    out.writeInt(entrySet4.size());
                    for (Entry<Integer, Integer> entry2 : entrySet4) {
                        out.writeInt(entry2.getKey());
                        out.writeInt(entry2.getValue());
                    }
                }
            }
        }

        for (int index = 0; index < numVertices; index++) {
            for (int index2 = 0; index2 < numLabels; index2++) {
                out.writeInt(getVertexNeighborLabelPos(index, index2));
            }
        }
    }

    public void readExternal (ObjectInput in) throws IOException, ClassNotFoundException {

        System.out.println("@DEBUG_CONF In UnsafeCSRGraphSearch.readExternal()");

        // Fields from AbstractMainGraph
        // isMultiGraph is covered by UnsafeCSRMainGraph and set to false in its build method
        numVertices = in.readLong();
        numEdges = in.readLong();
        isEdgeLabelled = in.readBoolean();
        isFloatLabel = in.readBoolean();
        isBinary = in.readBoolean();
        name = (String) in.readObject();


        // Fields from UnsafeCSRMainGraph
        built = in.readBoolean();

        if (built){
            verticesIndex = UNSAFE.allocateMemory((numVertices+1L) * INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES);
            verticesIndexLabel = UNSAFE.allocateMemory((numVertices + 1L) * INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES);
            edgesIndex       = UNSAFE.allocateMemory(numEdges * INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES);
            edgesIndexSource = UNSAFE.allocateMemory(numEdges * INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES);

            // DEBUG canary values to detect buffer overflow
            UNSAFE.putInt(verticesIndex + ((numVertices + 1L) *INT_SIZE_IN_BYTES), -99999);
            UNSAFE.putInt(verticesIndexLabel + ((numVertices + 1L) *INT_SIZE_IN_BYTES), -99999);
            UNSAFE.putInt(edgesIndex + (numEdges *INT_SIZE_IN_BYTES), -99999);
            UNSAFE.putInt(edgesIndexSource + (numEdges *INT_SIZE_IN_BYTES), -99999);

            for (long index = 0; index <= numVertices; index ++){
                // verticesIndex
                setVertexPos(index, in.readInt());
                // verticesIndexLabel
                // TODO add a safety check
                // TODO the superclass should take a long parameter, but this would require changing a central interface
                setVertexLabel(index, in.readInt());
            }

            for (long index = 0; index < numEdges; index ++) {
                // edgesIndex
                setEdgeDst((int) index, in.readInt());
                // edgesIndexSource
                setEdgeSource((int) index, in.readInt());
            }

            // DEBUG canary values to detect buffer overflow
            if (UNSAFE.getInt(verticesIndex + ((numVertices + 1) *INT_SIZE_IN_BYTES)) != -99999){
                throw new RuntimeException("Canary 1 is dead"); }
            if (UNSAFE.getInt(verticesIndexLabel + ((numVertices + 1) *INT_SIZE_IN_BYTES)) != -99999){
                throw new RuntimeException("Canary 2 is dead"); }
            if (UNSAFE.getInt(edgesIndex + (numEdges *INT_SIZE_IN_BYTES)) != -99999){
                throw new RuntimeException("Canary 3 is dead"); }
            if (UNSAFE.getInt(edgesIndexSource + (numEdges *INT_SIZE_IN_BYTES)) != -99999){
                throw new RuntimeException("Canary 4 is dead"); }
        }

        // Fields of this class
        numLabels = in.readInt();
        fastNeighbors = in.readBoolean();

        int size = in.readInt();
        if (size < 0){
            reverseVertexlabelCount = null;
        } else {
            reverseVertexlabelCount = HashIntIntMaps.newMutableMap();
            for (int i = 0; i < size; i++) {
                int key = in.readInt();
                int value = in.readInt();
                reverseVertexlabelCount.put(key, value);
            }
        }

        size = in.readInt();
        if (size < 0) {
            reverseVertexlabel = null;
        } else {
            reverseVertexlabel = HashIntObjMaps.newMutableMap();
            for (int i = 0; i < size; i++) {
                int key = in.readInt();
                int listSize = in.readInt();
                IntArrayList list = null;
                if (listSize >= 0) {
                    list = new IntArrayList(listSize);
                    for (int j = 0; j < listSize; j++) {
                        list.add(in.readInt());
                    }
                }
                reverseVertexlabel.put(key, list);
            }
        }

        size = in.readInt();
        if (size < 0){
            neighborhoodMap = null;
        } else {
            neighborhoodMap = HashIntObjMaps.newMutableMap();
            for (int i = 0; i < size; i++) {
                int key = in.readInt();
                int mapSize = in.readInt();
                HashIntIntMap map = null;
                if (mapSize >= 0) {
                    map = HashIntIntMaps.newMutableMap(mapSize);
                    for (int j = 0; j < mapSize; j++) {
                        int mapKey = in.readInt();
                        int mapValue = in.readInt();
                        map.put(mapKey, mapValue);
                    }
                }
                neighborhoodMap.put(key, map);
            }
        }

        vertexNeighLabelPos = UNSAFE.allocateMemory((numVertices*numLabels + 1L) * INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES);

        // DEBUG canary value to detect buffer overflow
        UNSAFE.putInt(vertexNeighLabelPos + ((numVertices*numLabels + 1L) *INT_SIZE_IN_BYTES), -99999);

        for (int index = 0; index < numVertices; index++) {
            for (int index2 = 0; index2 < numLabels; index2++) {
                setVertexNeighborLabelPos(index, index2, in.readInt());
            }
        }
        setVertexNeighborLabelPos((int)numVertices,0,getVertexPos(numVertices));

        // DEBUG canary value to detect buffer overflow
        if (UNSAFE.getInt(vertexNeighLabelPos + ((numVertices*numLabels + 1L) *INT_SIZE_IN_BYTES)) != -99999){
            throw new RuntimeException("Canary 5 is dead"); }
    }
}