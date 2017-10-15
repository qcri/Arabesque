package io.arabesque.graph;

import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntIterator;
import net.openhft.koloboke.collect.map.hash.HashIntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.function.IntConsumer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * A Vertex can have multiple labels.
 * Created by siganos on 6/5/16.
 */
public class UnsafeCSRGraphSearchEdgeLabelMulti
            extends UnsafeCSRGraphSearchEdgeLabel {
    protected HashIntObjMap<IntArrayList> multiVertexLabel; //TODO convert to UNSAFE.
    private long vertexMultipleEdgeLabelPos;

    private int multiLabelLimit;

    public UnsafeCSRGraphSearchEdgeLabelMulti(String name) {
        super(name);
    }

    public UnsafeCSRGraphSearchEdgeLabelMulti(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public UnsafeCSRGraphSearchEdgeLabelMulti(Path filePath) throws IOException {
        super(filePath);
    }

    public UnsafeCSRGraphSearchEdgeLabelMulti(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    void setVertexMultipleNeighbor(int index, int value){
        if (index > multiLabelLimit || index < 0 ) {
            throw new RuntimeException("Accessing above the limits:"+index);
        }
        UNSAFE.putInt(vertexMultipleEdgeLabelPos + (index * INT_SIZE_IN_BYTES), value);
    }

    private int getEdgeDstMultiLabel(int index) {
        if (index > multiLabelLimit || index < 0 ){
            throw new RuntimeException("Above limit edges1:"+index+" Actual limit is:"+ multiLabelLimit);
        }

        return UNSAFE.getInt(vertexMultipleEdgeLabelPos+(index*INT_SIZE_IN_BYTES));
    }

    @Override
    public void build() {
        super.build();
        multiVertexLabel = HashIntObjMaps.newMutableMap((int) numVertices);
    }

    protected boolean vertexHasLabel(int destinationLabel, int destVertexId) {
        return multiVertexLabel.get(destVertexId).contains(destinationLabel); //Should be a couple only.
    }

    /**
     * Should be one per thread accessing the graph.
     *
     * @return
     */
    @Override
    public synchronized IntIterator createNeighborhoodSearchIterator() {
        return new MyIteratorMulti(this);
    }


    @Override
    /**
     * Adding the reverse label only.
     */
    protected int parse_vertex(StringTokenizer tokenizer, int prev_vertex_id, int edges_position) {
        int vertexId = Integer.parseInt(tokenizer.nextToken());
        if (prev_vertex_id + 1 != vertexId) {
            throw new RuntimeException("Input graph isn't sorted by vertex id, or vertex id not sequential\n " +
                "Expecting:" + (prev_vertex_id + 1) + " Found:" + vertexId);
        }
        setVertexPos(vertexId, edges_position);

        String s = tokenizer.nextToken();
        while (!s.equals("e")) {
            int vertexLabel = Integer.parseInt(s);
            //setVertexLabel(vertexId, vertexLabel);
            reverseVertexlabelCount.addValue(vertexLabel, 1, 0);

            // First, forward direction.
            IntArrayList list = multiVertexLabel.get(vertexId);
            if (list == null){
                list = new IntArrayList(4);
                multiVertexLabel.put(vertexId,list);
            }
            list.add(vertexLabel);

            // Second, reverse direction.
            list = reverseVertexlabel.get(vertexLabel);
            if (list == null) {
                list = new IntArrayList(1024);
                reverseVertexlabel.put(vertexLabel, list);
            }
            list.add(vertexId);
            s = tokenizer.nextToken();
        }
        return vertexId;
    }

    /**
     * We cannot use the super method because it assumes that a vertex has a single label, which breaks
     * the logic of the code.
     */
    @Override
    void end_reading() {
        //super.end_reading();
        multiLabelLimit = computeNumberOfMultiNeighborhood();
        multiLabelLimit++;//For the extra value we add to the end.
        System.out.println("Required space:"+ multiLabelLimit);
        vertexMultipleEdgeLabelPos = UNSAFE.allocateMemory(multiLabelLimit * INT_SIZE_IN_BYTES);
        //System.out.println("Start building now next reference!!!");
        vertexNeighLabelPos = UNSAFE.allocateMemory((numVertices*numLabels+1L) * INT_SIZE_IN_BYTES);

        int neigh_label=0;

        HashMap<Integer,IntArrayList> one = new HashMap<>(numLabels);

        for (int i=0;i<numVertices;i++){
            //System.out.println("\t doing: "+i);
            int neigh     = getVertexPos(i);
            int neigh_end = getVertexPos(i+1);

            myclear(one);

            //boolean has_neighbors = false;
            while (neigh < neigh_end) {
                int _vertex_id = getEdgeDst(neigh);
                IntArrayList lls = multiVertexLabel.get(_vertex_id);
                if (lls == null) {
                    throw new RuntimeException("No label(s) for vertex:" + getEdgeDst(neigh));
                }

                for (int j = 0; j < lls.size(); j++) {
                    int _label = lls.getUnchecked(j);
                    one.get(_label).add(_vertex_id);
                }
                neigh++;
            }

            for (int k = 0; k < numLabels; k++) {
                IntArrayList _neigh = one.get(k);
                _neigh.sort();
                //First, write the index, how to find.
                //System.out.println("Setting label pos:"+i+" "+k+" "+neigh_label);
                //Second, write the actual neighbors.
                for (int z = 0; z < _neigh.size(); z++) {
                   // System.out.println("\tAdding:"+_neigh.getUnchecked(z));
                    setVertexMultipleNeighbor(neigh_label, _neigh.getUnchecked(z));
                    neigh_label += 1;
                }
                setVertexNeighborLabelPos(i, k, neigh_label);
            }
        }
        //Add the last so that we don't care about limits.
        setVertexNeighborLabelPos((int)numVertices,0,neigh_label);
        //System.out.println("Used space:"+neigh_label);
        //System.out.println("Done:"+multiLabelLimit);
        //From super...
        add_last_vertex();
    }

    private int computeNumberOfMultiNeighborhood() {
        int counter = 0;
        for (int i =0; i< numVertices;i++){
            counter += (neighborhoodSize(i)*multiVertexLabel.get(i).size());
        }
        return counter;
    }

    private void myclear(HashMap<Integer, IntArrayList> one) {
        one.clear();

        //Create again the labels.
        for (int i = 0; i< numLabels; i++){
            one.put(i,new IntArrayList());
        }
    }

    @Override
    public boolean isNeighborVertexWithLabel(int sourceVertexId, int destVertexId, int destinationLabel) {
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
            final int key = binarySearch0(vertexMultipleEdgeLabelPos, start, end, destVertexId);
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

            final int key = binarySearch0(vertexMultipleEdgeLabelPos, start, end, destVertexId);
            if(key >= 0){
                return true;
            }
        }
        return false;
    }

    @Override
    public void setIteratorForNeighborsWithLabel(int vertexId, int vertexLabel, IntIterator _one) {
        MyIterator one = (MyIterator) _one;
        one.setVertexLabel(vertexId,vertexLabel);
    }


    public class MyIteratorMulti extends MyIterator {
        protected UnsafeCSRGraphSearchEdgeLabelMulti graph;

        MyIteratorMulti(UnsafeCSRGraphSearchEdgeLabelMulti graph) {
            super(graph);
            this.graph = graph;
        }


        @Override
        public int nextInt() {
            return graph.getEdgeDstMultiLabel(pos++);
        }

        @Override
        public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public boolean hasNext() {
//            System.out.println("\t"+pos+"  -> "+end);
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

