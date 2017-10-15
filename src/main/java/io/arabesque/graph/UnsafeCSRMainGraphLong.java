package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import net.openhft.koloboke.function.IntConsumer;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.StringTokenizer;

/**
 * Created by siganos on 3/8/16.
 */
public class UnsafeCSRMainGraphLong
                 extends AbstractMainGraph {
    static final sun.misc.Unsafe UNSAFE;
    static {
        try {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("UnsafeArrayReads: Failed to " +
                "get unsafe", e);
        }
    }

    private static final Logger LOG = Logger.getLogger(UnsafeCSRMainGraph.class);

    final static long INT_SIZE_IN_BYTES = 4;
    final static long LONG_SIZE_IN_BYTES = 8;
    private boolean built = false;

    // The vertices array.
    private long verticesIndex;
    private long verticesIndexLabel;

    // The edges array, value is the Dest.
    protected long edgesIndex;
    // Edges array, value is the source.
    private long edgesIndexSource;

    boolean isMultigraph;

    public UnsafeCSRMainGraphLong(String name) {
        super(name);
    }

    public UnsafeCSRMainGraphLong(String name, boolean a, boolean b) {
        super(name, a, b);
    }

    public UnsafeCSRMainGraphLong(Path filePath) throws IOException {
        super(filePath);
    }

    public UnsafeCSRMainGraphLong(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    protected void build() {
        // WHEN LOADING ADD AN EXTRA VERTEX SO THAT WE DON'T CARE
        // FOR BOUNDARY FOR THE LAST VERTEX.
        // We require to know as input the number of vertices and edges.
        // Else we fail.
        built = true;
        Configuration conf = Configuration.get();
        numEdges    = conf.getNumberEdges();
        numVertices = conf.getNumberVertices();
        isMultigraph = conf.isGraphMulti();

        //System.out.println("Using UNSAFE Graph");

        if (numEdges < 0 || numVertices < 0) {
            throw new RuntimeException("We require the number of edges and vertices");
        }

        verticesIndex = UNSAFE.allocateMemory((numVertices+1L) * LONG_SIZE_IN_BYTES);

        // Currently if edge labelled then we have no vertex label (This could change).
//        if (!isEdgeLabelled) {
            verticesIndexLabel = UNSAFE.allocateMemory((numVertices + 1L) * INT_SIZE_IN_BYTES);
//        }

        edgesIndex       = UNSAFE.allocateMemory(numEdges * INT_SIZE_IN_BYTES);
        edgesIndexSource = UNSAFE.allocateMemory(numEdges * INT_SIZE_IN_BYTES);

    }

    protected void setVertexPos(long index, long value) {
        //Allow equal due to the extra virtual vertex i add.
        if (index > numVertices || index < 0){
            throw new RuntimeException("Above limit vertex:"+index);
        }
        UNSAFE.putLong(verticesIndex + (index*LONG_SIZE_IN_BYTES), value);
    }

    protected long getVertexPos(long index) {
        if (index>numVertices || index < 0 ){
            throw new RuntimeException("Above limit vertex:Get"+index);
        }
        return UNSAFE.getLong(verticesIndex+(index*LONG_SIZE_IN_BYTES));
    }

    protected void setVertexLabel(long index, int value) {
        if (index >= numVertices || index < 0 ){
            throw new RuntimeException("Above limit vertex label:"+index);
        }

        UNSAFE.putInt(verticesIndexLabel + (index*INT_SIZE_IN_BYTES), value);
    }

    public int getVertexLabel(int index) {
        if (index>=numVertices || index < 0 ){
            throw new RuntimeException("Above limit vertex:Get"+index);
        }

        return UNSAFE.getInt(verticesIndexLabel+(index*INT_SIZE_IN_BYTES));
    }

    public int getEdgeDst(long index){
        if (index>=numEdges || index < 0 ){
            throw new RuntimeException("Above limit edges1"+index);
        }

        return UNSAFE.getInt(edgesIndex+(index*INT_SIZE_IN_BYTES));
    }

    @Override
    public int neighborhoodSize(int vertexId) {
        return (int) (getVertexPos(vertexId+1) - getVertexPos(vertexId));
    }

    public void setEdgeDst(long index,int value){
        if (index >= numEdges || index < 0 ){
            throw new RuntimeException("Above limit edges:"+index);
        }

        UNSAFE.putInt(edgesIndex+(index*INT_SIZE_IN_BYTES),value);
    }

    public int getEdgeSource(int index) {
        if (index>=numEdges || index < 0 ){
            throw new RuntimeException("Above limit edges2"+index);
        }

        return UNSAFE.getInt(edgesIndexSource+(index*INT_SIZE_IN_BYTES));
    }

    @Override
    public int getEdgeDst(int edgeId) {
        throw new RuntimeException("Shouldn't be used for search");
    }

    public void setEdgeSource(long index, int value) {
        if (index >= numEdges || index < 0 ){
            throw new RuntimeException("Above limit edges Source:"+index);
        }

        UNSAFE.putInt(edgesIndexSource+(index*INT_SIZE_IN_BYTES),value);
    }

    @Override
    public int getEdgeLabel(int edgeId) {
        throw new RuntimeException("Doesn't have a label");
    }

    public void destroy() {
        UNSAFE.freeMemory(verticesIndex);
//        if (!isEdgeLabelled) {
            UNSAFE.freeMemory(verticesIndexLabel);
//        }
        UNSAFE.freeMemory(edgesIndex);
        UNSAFE.freeMemory(edgesIndexSource);
    }

    @Override
    public void reset() {
        if (!built){
            build();
            //Not initialised yet, so no need to reset.
            return;
        }
        throw new RuntimeException("We don't have reset...");
    }

    @Override
    public void processVertexNeighbors(int vertexId,
                                      IntConsumer consumer) {
        final long start = getVertexPos(vertexId);
        final long end = getVertexPos(vertexId+1);

        for (long i = start; i<end; i++){
            consumer.accept(getEdgeDst(i));
        }

    }

    @Override
    public void processEdgeNeighbors(int vertexId,
                                    IntConsumer consumer) {
        final long start = getVertexPos(vertexId);
        final long end = getVertexPos(vertexId+1);

        for (long i = start; i<end; i++) {
            throw new RuntimeException("Only for search, and we don't use anything here.");
            //consumer.accept(i);
        }
    }

    @Override
    protected void readFromInputStreamText(InputStream is) throws IOException {
        long start = 0;

        if (LOG.isInfoEnabled()) {
            start = System.currentTimeMillis();
            LOG.info("Initializing");
        }

        int prev_vertex_id = -1;
        long edges_position = 0;

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new BOMInputStream(is)));

        String line = reader.readLine();
        boolean firstLine = true;

        while (line != null) {
            StringTokenizer tokenizer = new StringTokenizer(line);

            if (firstLine) {
                firstLine = false;

                if (line.startsWith("#")) {
                    throw new RuntimeException("Don't have preamble in the file");
                }
            }

            int vertexId = parse_vertex(tokenizer, prev_vertex_id,edges_position);
            prev_vertex_id = vertexId;

            edges_position = parse_edge(tokenizer,vertexId, edges_position);
            line = reader.readLine();
        }
        reader.close();
        // Add the last one, so that we don't care about boundaries of edges.
        setVertexPos(prev_vertex_id+1,edges_position);

        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }

        end_reading();
    }

    protected int parse_vertex(StringTokenizer tokenizer, int prev_vertex_id, long edges_position) {
        int vertexId = Integer.parseInt(tokenizer.nextToken());
        int vertexLabel = Integer.parseInt(tokenizer.nextToken());
        if (prev_vertex_id + 1 != vertexId) {
            throw new RuntimeException("Input graph isn't sorted by vertex id, or vertex id not sequential\n " +
                "Expecting:" + (prev_vertex_id + 1) + " Found:" + vertexId);
        }

        setVertexPos(vertexId,edges_position);
        setVertexLabel(vertexId,vertexLabel);
        return vertexId;
    }

    void end_reading() {

    }

    protected long parse_edge(StringTokenizer tokenizer, int vertexId, long edges_position) {
        int prev_edge = -1;

        while (tokenizer.hasMoreTokens()) {
            int neighborId = Integer.parseInt(tokenizer.nextToken());
            if (prev_edge >= 0 && prev_edge > neighborId) {
                throw new RuntimeException("The edges need to be sorted for unsafe");
            }
            prev_edge = neighborId;
            //We only add one direction since we assume undirected graph.
            if (vertexId <= neighborId) {
                setEdgeSource(edges_position, vertexId);
                setEdgeDst(edges_position, neighborId);

                edges_position++;
            }
        }

        return edges_position;
    }

    @Override
    protected void readFromInputStreamBinary(InputStream is) throws IOException {
        long start = 0;

        BufferedInputStream a_ = new BufferedInputStream(is);
        DataInputStream in = new DataInputStream(a_);

        int vertex_id = 0;
        int edge_pos = 0;

        while (in.available() > 0) {
            int c_v_id = in.readInt();
            int label = in.readInt();
            int num = in.readInt();

            setVertexPos(c_v_id,edge_pos);
            setVertexLabel(c_v_id,label);

            //Sanity for correct input.
            if (vertex_id+1!=c_v_id){
                throw new RuntimeException("Vertices should be strictly incremental");
            }
            vertex_id = c_v_id;

            int prev_neighbor = -1;
            while (num > 0) {
                final int neigbor = in.readInt();

                //Sanity for correct input.
                if (prev_neighbor > neigbor) {
                    throw new RuntimeException("Edges should be ordered by increasing id");
                }

                setEdgeSource(edge_pos, vertex_id);
                setEdgeDst(edge_pos, neigbor);
                prev_neighbor = neigbor;
                edge_pos++;
            }

        }
        // Last to avoid boundary problems
        setVertexPos(vertex_id+1,edge_pos);

        in.close();
        a_.close();

        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }
    }

    @Override
    public boolean isEdgeLabelled() {
        return false;
    }

    @Override
    public boolean isMultiGraph() {
        return isMultigraph;
    }

    @Override
    public void forEachEdgeId(int v1, int v2, IntConsumer intConsumer) {
       throw new RuntimeException("Not used for search our target");
    }

    @Override
    public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {

        return (getEdgeSource(edge1Id) == getEdgeSource(edge2Id) ||
                getEdgeSource(edge1Id) == getEdgeDst(edge2Id) ||
                getEdgeDst(edge1Id) == getEdgeSource(edge2Id) ||
                getEdgeDst(edge1Id) == getEdgeDst(edge2Id));
    }


    @Override
    public boolean isNeighborVertex(int v1, int v2) {
        int minv;
        int maxv;

        if (v1 < v2) {
            minv = v1;
            maxv = v2;
        } else {
            minv = v2;
            maxv = v1;
        }

        final long start = getVertexPos(minv);
        final long end = getVertexPos(minv+1);
        //We need to find the maxv!!! Damn... To work neighborhood must be sorted...
        long key = binarySearch0l(edgesIndex,start,end,maxv);

        return key>=0;
    }

    @Override
    public MainGraph addVertex(Vertex vertex) {
        throw new RuntimeException("Should not be used. Exists for testing other classes only");
    }

    @Override
    public MainGraph addEdge(Edge edge) {
        throw new RuntimeException("Using only for tests now and old code...");
    }

    protected int binarySearch0(long index, int fromIndex, int toIndex,
                                     int key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            //System.out.println("Before:"+mid);
            int midVal = UNSAFE.getInt(index+(mid*INT_SIZE_IN_BYTES));
            //System.out.println("After:"+midVal);
            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    protected long binarySearch0l(long index, long fromIndex, long toIndex,
                                int key) {
        long low = fromIndex;
        long high = toIndex - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;
            //System.out.println("Before:"+mid);
            int midVal = UNSAFE.getInt(index+(mid*INT_SIZE_IN_BYTES));
            //System.out.println("After:"+midVal);
            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

}