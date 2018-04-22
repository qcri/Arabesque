package io.arabesque.graph;

import io.arabesque.utils.collection.IntArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Represent the Query graph. Tiny graph not performance bottleneck.
 * Created by siganos on 4/20/16.
 */
public class BasicMainGraphQuery extends BasicMainGraph {
    private static final Logger LOG = Logger.getLogger(BasicMainGraphQuery.class);

    private Int2ObjectOpenHashMap<IntArrayList> smallerIds;

    public BasicMainGraphQuery() {}

    public BasicMainGraphQuery(String name) {
        super(name);
    }

    public BasicMainGraphQuery(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public BasicMainGraphQuery(Path filePath) throws IOException {
        super(filePath);
    }

    public BasicMainGraphQuery(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    public IntSet getAllLabelsNeighbors(int vertex) {
        IntCollection vertexNeighbours = getVertexNeighbours(vertex);

        if (vertexNeighbours==null){
            return null;
        }

        IntSet labels = HashIntSets.newMutableSet();

        for (int one:vertexNeighbours){
            labels.add(getVertexLabel(one));
        }
        return labels;
    }

    /**
     * Return true if a label on a edge is a star.
     * @return
     */
    public boolean has_star_on_edges() {
        return negative_edge_label;
    }


    public int getNeighborhoodSizeWithLabel(int vertex, int label) {
        IntCollection vertexNeighbours = getVertexNeighbours(vertex);

        if (vertexNeighbours==null){
            return 0;
        }

        int count = 0;

        for (int one:vertexNeighbours){
            if (getVertexLabel(one) == label){
                count++;
            }
        }

        return count;
    }

//    public IntArrayList getEdgesLabel(int one, int two) {
//        ReclaimableIntCollection edgeIds = getEdgeIds(one, two);
//        if (edgeIds == null){
//            return null;
//        }
//
//        IntArrayList edgeLabelsToParent = new IntArrayList();
//        for (int edgeId : edgeIds) {
//            edgeLabelsToParent.add(getEdgeLabel(edgeId));
//        }
//        return edgeLabelsToParent;
//    }

    @Override
    public MainGraph addVertex(Vertex vertex) {
        ensureCanStoreNewVertex();
        vertexIndexF[(int)numVertices++] = vertex;

        return this;
    }

    public MainGraph addEdge(Edge edge) {

        if (edge.getEdgeId() == -1) {
            edge.setEdgeId((int)numEdges);
        } else if (edge.getEdgeId() != numEdges) {
            throw new RuntimeException("Sanity check, edge with id " + edge.getEdgeId() + " added at position " + numEdges);
        }

        ensureCanStoreNewEdge();
        ensureCanStoreUpToVertex(Math.max(edge.getSourceId(), edge.getDestinationId()));
        edgeIndexF[(int)numEdges++] = edge;
        addToNeighborhood(edge);

        return this;
    }

    @Override
    protected void readFromInputStream(InputStream is) throws IOException {
        if (isBinary && isMultiGraph) {
            readFromInputStreamBinary(is);
        } else {
            readFromInputStreamText(is);
        }
    }

    protected void readFromInputStreamText(InputStream is) {
        long start = 0;

        System.out.println("BasicMainGraphQuery.readFromInputStreamText");
        if (LOG.isInfoEnabled()) {
            start = System.currentTimeMillis();
            LOG.info("Initializing");
        }

        smallerIds = new Int2ObjectOpenHashMap<>();

        int prev_vertex_id = -1;
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new BOMInputStream(is)));

            String line = reader.readLine();
            boolean firstLine = true;

            while (line != null) {
                StringTokenizer tokenizer = new StringTokenizer(line);

                if (firstLine) {
                    firstLine = false;

                    if (line.startsWith("#")) {
                        LOG.info("Found hints regarding number of vertices and edges");
                        // Skip #
                        tokenizer.nextToken();

                        int numVertices = Integer.parseInt(tokenizer.nextToken());
                        int numEdges = Integer.parseInt(tokenizer.nextToken());

                        LOG.info("Hinted numVertices=" + numVertices);
                        LOG.info("Hinted numEdges=" + numEdges);

                        prepareStructures(numVertices, numEdges);

                        line = reader.readLine();
                        continue;
                    }
                }

                Vertex vertex = parseVertex(tokenizer);
                if (prev_vertex_id + 1 != vertex.getVertexId()) {
                    throw new RuntimeException("Input graph isn't sorted by vertex id, or vertex id not sequential\n " +
                            "Expecting:" + (prev_vertex_id + 1) + " Found:" + vertex.getVertexId());
                }
                prev_vertex_id = vertex.getVertexId();
                addVertex(vertex);

                int vertexId = vertex.getVertexId();

                boolean scanningSmallerIds = false;
                while (tokenizer.hasMoreTokens()) {
                    String next = tokenizer.nextToken();
                    if (next.equalsIgnoreCase("*")){
                        scanningSmallerIds = true;
                    }
                    else if (!scanningSmallerIds) {
                        Edge edge = parseEdge(Integer.parseInt(next), tokenizer, vertexId);
                        addEdge(edge);
                    }
                    else {
                        IntArrayList ids = smallerIds.get(vertexId);
                        if (ids == null){
                            ids = new IntArrayList();
                            smallerIds.put(vertexId, ids);
                        }
                        ids.add(Integer.parseInt(next));
                    }
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }
        //System.out.println("Finish reading:"+negative_edge_label);
    }

    public IntArrayList getSmallerIds (int vertexId){
        return smallerIds.get(vertexId);
    }

    public void write(ObjectOutput out)
            throws IOException{
        super.write(out);

        if(smallerIds == null){
            out.writeInt(-1);
        } else {
            ObjectSet<Map.Entry<Integer, IntArrayList>> entries = smallerIds.entrySet();
            out.writeInt(entries.size());
            for (Map.Entry<Integer, IntArrayList> entry : entries){
                out.writeInt(entry.getKey());
                IntArrayList list = entry.getValue();
                if (list == null){
                    out.writeInt(-1);
                } else {
                    int size = list.size();
                    out.writeInt(size);
                    for (int i=0; i < size; i++){
                        out.writeInt(list.get(i));
                    }
                }
            }
        }
    }

    public void read(ObjectInput in) throws IOException, ClassNotFoundException {
        super.read(in);
        System.out.println("BasicMainGraphQuery.read");
        int size = in.readInt();
        if (size < 0){
            smallerIds = null;
        } else {
            smallerIds = new Int2ObjectOpenHashMap<>(size);
            for (int i=0; i < size; i++){
                int key = in.readInt();
                IntArrayList list = null;
                int sizeList = in.readInt();
                if (sizeList >= 0){
                    list = new IntArrayList();
                    for (int j=0; j < sizeList; j++){
                        list.add(in.readInt());
                    }
                }
                smallerIds.put(key,list);
            }
        }
    }


//    public void checkMe(){
//        for (int i = 0;i < numVertices; i++){
//            System.out.println("Vertex:"+i);
//            IntCollection vertexNeighbors = getVertexNeighbors(i);
//            System.out.print("\t");
//            for (int k:vertexNeighbors){
//                System.out.print(k+" ");
//            }
//            System.out.println("\n");
//        }
//    }
}
