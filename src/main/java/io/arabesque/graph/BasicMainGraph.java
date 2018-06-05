package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import jdk.nashorn.internal.objects.annotations.Getter;

public class BasicMainGraph extends AbstractMainGraph {
    protected static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

    protected static final int INITIAL_ARRAY_SIZE = 4096;

    protected Vertex[] vertexIndexF;
    protected Edge[] edgeIndexF;

    protected VertexNeighbourhood[] vertexNeighbourhoods;
    protected HashMap<Integer, Vertex> vertexIdToVertexMap = new HashMap<>();

    protected boolean negative_edge_label; //Used for search.

    public BasicMainGraph() {}

    public BasicMainGraph(String name) {
        super(name);
    }

    public BasicMainGraph(Path filePath) throws IOException {
        super(filePath);
    }

    public BasicMainGraph(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
    }

    public BasicMainGraph(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
        super(name, isEdgeLabelled, isMultiGraph);
    }

    public BasicMainGraph(Path filePath, boolean isEdgeLabelled, boolean isMultiGraph)
            throws IOException {
        this(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
        super.init(filePath, isEdgeLabelled, isMultiGraph);
    }

    public BasicMainGraph(org.apache.hadoop.fs.Path hdfsPath, boolean isEdgeLabelled, boolean isMultiGraph)
            throws IOException {
        this(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
        super.init(hdfsPath, isEdgeLabelled, isMultiGraph);
    }

    protected void prepareStructures(int numVertices, int numEdges) {
        ensureCanStoreNewVertices(numVertices);
        ensureCanStoreNewEdges(numEdges);
    }

    @Override
    public void reset() {
        numVertices = 0;
        numEdges = 0;
        vertexIndexF = null;
        edgeIndexF = null;
        vertexNeighbourhoods = null;
    }

    protected void ensureCanStoreNewVertices(int numVerticesToAdd) {
        int newMaxVertexId = (int)(numVertices + numVerticesToAdd);

        ensureCanStoreUpToVertex(newMaxVertexId);
    }

    protected void ensureCanStoreUpToVertex(int maxVertexId) {
        int targetSize = maxVertexId + 1;

        if (vertexIndexF == null) {
            vertexIndexF = new Vertex[Math.max(targetSize, INITIAL_ARRAY_SIZE)];
        } else if (vertexIndexF.length < targetSize) {
            vertexIndexF = Arrays.copyOf(vertexIndexF, getSizeWithPaddingWithoutOverflow(targetSize, vertexIndexF.length));
        }

        if (vertexNeighbourhoods == null) {
            vertexNeighbourhoods = new VertexNeighbourhood[Math.max(targetSize, INITIAL_ARRAY_SIZE)];
        } else if (vertexNeighbourhoods.length < targetSize) {
            vertexNeighbourhoods = Arrays.copyOf(vertexNeighbourhoods, getSizeWithPaddingWithoutOverflow(targetSize, vertexNeighbourhoods.length));
        }
    }

    private int getSizeWithPaddingWithoutOverflow(int targetSize, int currentSize) {
        if (currentSize > targetSize) {
            return currentSize;
        }

        int sizeWithPadding = Math.max(currentSize, 1);

        while (true) {
            int previousSizeWithPadding = sizeWithPadding;

            // Multiply by 2
            sizeWithPadding <<= 1;

            // If we saw an overflow, return simple targetSize
            if (previousSizeWithPadding > sizeWithPadding) {
                return targetSize;
            }

            if (sizeWithPadding >= targetSize) {
                return sizeWithPadding;
            }
        }
    }

    protected void ensureCanStoreNewVertex() {
        ensureCanStoreNewVertices(1);
    }

    private void ensureCanStoreNewEdges(int numEdgesToAdd) {
        if (edgeIndexF == null) {
            edgeIndexF = new Edge[Math.max(numEdgesToAdd, INITIAL_ARRAY_SIZE)];
        } else if (edgeIndexF.length < numEdges + numEdgesToAdd) {
            int targetSize = edgeIndexF.length + numEdgesToAdd;
            edgeIndexF = Arrays.copyOf(edgeIndexF, getSizeWithPaddingWithoutOverflow(targetSize, edgeIndexF.length));
        }
    }

    protected void ensureCanStoreNewEdge() {
        ensureCanStoreNewEdges(1);
    }

    @Override
    public boolean isNeighborVertex(int v1, int v2) {
        VertexNeighbourhood v1Neighbourhood = vertexNeighbourhoods[v1];

        return v1Neighbourhood != null && v1Neighbourhood.isNeighbourVertex(v2);
    }

    /**
     * @param vertex
     * @return
     */
    @Override
    public MainGraph addVertex(Vertex vertex) {
        ensureCanStoreNewVertex();
        vertexIndexF[(int)numVertices++] = vertex;
        vertexIdToVertexMap.put(vertex.getVertexId(), vertex);

        return this;
    }

    @Override
    public Vertex[] getVertices() {
        return vertexIndexF;
    }

    @Getter
    @Override
    public Vertex getVertex(int vertexId) {
        if(vertexId < vertexIndexF.length)
            return vertexIndexF[vertexId];
        return null;
    }

    @Override
    public Edge[] getEdges() {
        return edgeIndexF;
    }

    @Override
    public Edge getEdge(int edgeId) {
        return edgeIndexF[edgeId];
    }

    @Override
    public ReclaimableIntCollection getEdgeIds(int v1, int v2) {
        int minv;
        int maxv;

        // TODO: Change this for directed edges
        if (v1 < v2) {
            minv = v1;
            maxv = v2;
        } else {
            minv = v2;
            maxv = v1;
        }

        VertexNeighbourhood vertexNeighbourhood = this.vertexNeighbourhoods[minv];

        return vertexNeighbourhood.getEdgesWithNeighbourVertex(maxv);
    }

    @Override
    public void forEachEdgeId(int v1, int v2, IntConsumer intConsumer) {
        int minv;
        int maxv;

        // TODO: Change this for directed edges
        if (v1 < v2) {
            minv = v1;
            maxv = v2;
        } else {
            minv = v2;
            maxv = v1;
        }

        VertexNeighbourhood vertexNeighbourhood = this.vertexNeighbourhoods[minv];

        vertexNeighbourhood.forEachEdgeId(maxv, intConsumer);
    }

    @Override
    public MainGraph addEdge(Edge edge) {
        // Assuming input graph contains all edges but we treat it as undirected
        // TODO: What if input only contains one of the edges? Should we enforce
        // this via a sanity check?
        // TODO: Handle this when directed graphs
        if (edge.getSourceId() > edge.getDestinationId()) {
            return this;
        }

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
//*
    protected void readFromInputStream(InputStream is) throws IOException {
        System.out.println("BasicMainGraph.readFromInputStream");
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
                addVertex(vertex);

                int vertexId = vertex.getVertexId();

                while (tokenizer.hasMoreTokens()) {
                    Edge edge = parseEdge(tokenizer, vertexId);

		            //halt if vertex self loop is found
                    if( edge.getSourceId() == edge.getDestinationId()) {
                        LOG.error("The input graph contains vertices having self loops. Arabesque does not support self loops");
                        LOG.error("Self loop at vertex #: " + edge.getSourceId());
                        throw new RuntimeException("Graphs with self loops are not supported");
                    }

                    addEdge(edge);
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Edge parseEdge(StringTokenizer tokenizer, int vertexId) {
        int neighborId = Integer.parseInt(tokenizer.nextToken());

        return parseEdge(neighborId, tokenizer, vertexId);
    }

    protected Edge parseEdge(int neighborId, StringTokenizer tokenizer, int vertexId) {

        if (isEdgeLabelled) {
            if (isFloatLabel) {
                float edgeLabel = Float.parseFloat(tokenizer.nextToken());
                if (edgeLabel<0){
                    negative_edge_label = true;
                }
                return createEdge(vertexId, neighborId, edgeLabel);
            }
            int edgeLabel = Integer.parseInt(tokenizer.nextToken());
            if (edgeLabel<0){
                negative_edge_label = true;
            }
            return createEdge(vertexId, neighborId, edgeLabel);
        } else {
            return createEdge(vertexId, neighborId);
        }
    }

    protected Vertex parseVertex(StringTokenizer tokenizer) {
        int vertexId = Integer.parseInt(tokenizer.nextToken());
        int vertexLabel = Integer.parseInt(tokenizer.nextToken());

        return createVertex(vertexId, vertexLabel);
    }

    @Override
    public String toString() {
        return getName();
    }

    public String toDetailedString() {
        return "Vertices: " + Arrays.toString(vertexIndexF) + "\n Edges: " + Arrays.toString(edgeIndexF);
    }

    @Override
    public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
        Edge edge1 = edgeIndexF[edge1Id];
        Edge edge2 = edgeIndexF[edge2Id];

        return edge1.neighborWith(edge2);
    }

    @Override
    public boolean isNeighborEdge(int src1, int dest1, int edge2) {
        int src2 = edgeIndexF[edge2].getSourceId();

        if (src1 == src2) return true;

        int dest2 = edgeIndexF[edge2].getDestinationId();

        return (dest1 == src2 || dest1 == dest2 || src1 == dest2);
    }

    protected Vertex createVertex(int id, int label) {
        return new Vertex(id, label);
    }

    protected Vertex createVertex(int id, int label, int subgraph) {
        return new Vertex(id, label, subgraph);
    }

    protected Edge createEdge(int srcId, int destId) {
        return new Edge(srcId, destId);
    }

    protected Edge createEdge(int srcId, int destId, int label) {
        return new LabelledEdge(srcId, destId, label);
    }

    private VertexNeighbourhood createVertexNeighbourhood() {
        if (!isMultiGraph) {
            return new BasicVertexNeighbourhood();
        }
        else {
            return new MultiVertexNeighbourhood();
        }
    }

    @Override
    public VertexNeighbourhood getVertexNeighbourhood(int vertexId) {
        return vertexNeighbourhoods[vertexId];
    }

    @Override
    public IntCollection getVertexNeighbours(int vertexId) {
        VertexNeighbourhood vertexNeighbourhood = getVertexNeighbourhood(vertexId);

        if (vertexNeighbourhood == null) {
            return null;
        }

        return vertexNeighbourhood.getNeighbourVertices();
    }

    @Override
    public boolean isEdgeLabelled() {
        return isEdgeLabelled;
    }

    @Override
    public boolean isMultiGraph() {
        return isMultiGraph;
    }

    public String getName() {
        return name;
    }

    //***** Modifications coming from QFrag

    protected void readFromInputStreamText(InputStream is) {
        System.out.println("BasicMainGraph.readFromInputStreamText");

        //*
        long start = 0;

        if (LOG.isInfoEnabled()) {
            start = System.currentTimeMillis();
            LOG.info("Initializing");
        }

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

                while (tokenizer.hasMoreTokens()) {
                    Edge edge = parseEdge(tokenizer, vertexId);
                    addEdge(edge);
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
    }

    protected void readFromInputStreamBinary(InputStream is) throws IOException {
        long start = 0;

        if (LOG.isInfoEnabled()) {
            start = System.currentTimeMillis();
            LOG.info("Initializing");
        }

        // We require to know as input the number of vertices and edges.
        // Else we fail.
        Configuration conf = Configuration.get();
        long edges = conf.getNumberEdges();
        long vertices = conf.getNumberVertices();

        if (edges < 0 || vertices < 0) {
            throw new RuntimeException("For Binary require the number of edges and vertices");
        }

        edgeIndexF = new Edge[(int) edges];
        vertexNeighbourhoods = new VertexNeighbourhood[(int) vertices];
        vertexIndexF = new Vertex[(int) vertices];

        BufferedInputStream a_ = new BufferedInputStream(is);
        DataInputStream in = new DataInputStream(a_);

        while (in.available() > 0) {
            Edge edge = createEdge((int) numEdges, in.readInt(), in.readInt(), in.readInt());
            edgeIndexF[(int) numEdges] = edge;
            addToNeighborhood(edge);
            numEdges++;
        }

        // Dump vertexIndexF create (empty). QFrag probably needs this.
        // TODO: have something better for labels on vertices. Maybe if src_id and dst_id is the same.
        for (int i = 0; i < vertices; i++) {
            vertexIndexF[i] = createVertex(i, -1);
        }

        in.close();
        a_.close();
        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }
    }

    protected void addToNeighborhood(Edge edge) {
        try {
            VertexNeighbourhood vertexNeighbourhood = vertexNeighbourhoods[edge.getSourceId()];

            if (vertexNeighbourhood == null) {
                vertexNeighbourhood = createVertexNeighbourhood();
                vertexNeighbourhoods[edge.getSourceId()] = vertexNeighbourhood;
            }

            vertexNeighbourhood.addEdge(edge.getDestinationId(), edge.getEdgeId());
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Tried to access index " + edge.getSourceId() + " of array with size " + vertexNeighbourhoods.length);
            LOG.error("vertexIndexF.length=" + vertexIndexF.length);
            LOG.error("vertexNeighbourhoods.length=" + vertexNeighbourhoods.length);
            throw e;
        }

        try {
            VertexNeighbourhood vertexNeighbourhood = vertexNeighbourhoods[edge.getDestinationId()];

            if (vertexNeighbourhood == null) {
                vertexNeighbourhood = createVertexNeighbourhood();
                vertexNeighbourhoods[edge.getDestinationId()] = vertexNeighbourhood;
            }

            vertexNeighbourhood.addEdge(edge.getSourceId(), edge.getEdgeId());
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Tried to access index " + edge.getDestinationId() + " of array with size " + vertexNeighbourhoods.length);
            LOG.error("vertexIndexF.length=" + vertexIndexF.length);
            LOG.error("vertexNeighbourhoods.length=" + vertexNeighbourhoods.length);
            throw e;
        }
    }

    @Override
    public int getVertexLabel(int v) {
        return vertexIndexF[v].getVertexLabel();
    }

    public IntCollection getVertexNeighbors(int vertexId) {
        if(vertexNeighbourhoods[vertexId]!=null){
            return vertexNeighbourhoods[vertexId].getNeighbourVertices();
        }
        return null;
    }

    @Override
    public int getEdgeLabel(int edgeId) {
        LabelledEdge edge = (LabelledEdge) edgeIndexF[edgeId];
        return edge.getEdgeLabel();
    }

    @Override
    public int getEdgeSource(int edgeId) {
        return edgeIndexF[edgeId].getSourceId();
    }

    @Override
    public int getEdgeDst(int edgeId) {
        return edgeIndexF[edgeId].getDestinationId();
    }

    @Override
    public int neighborhoodSize(int vertexId) {
        return vertexNeighbourhoods[vertexId].getNeighbourVertices().size();
    }

    protected Edge createEdge(int edgeId, int srcId, int destId, int label) {
        return new LabelledEdge(edgeId, srcId, destId, label);
    }

    protected Edge createEdge(int srcId, int destId, float label) {
        return new FloatEdge(srcId, destId, label);
    }

    @Override
    public void processVertexNeighbors(int vertexId, IntConsumer intAddConsumer) {
        VertexNeighbourhood vertexNeighbourhood = vertexNeighbourhoods[vertexId];

        if (vertexNeighbourhood == null) {
            return;
        }
        vertexNeighbourhood.getNeighbourVertices().forEach(intAddConsumer);
    }

    @Override
    public void processEdgeNeighbors(int vertexId, IntConsumer intAddConsumer) {
        VertexNeighbourhood neighbourEdges = vertexNeighbourhoods[vertexId];
        if (neighbourEdges == null){
            return;
        }
        neighbourEdges.getNeighbourEdges().forEach(intAddConsumer);
    }

    public void write (ObjectOutput out)
            throws IOException {
        super.write(out);

        if (vertexNeighbourhoods == null){
            out.writeInt(-1);
        } else {
            int size = vertexNeighbourhoods.length;
            out.writeInt(size);
            if (size > 0){
                String className = vertexNeighbourhoods[0].getClass().getName();
                switch (className){
                    case "io.arabesque.graph.BasicVertexNeighbourhood": out.writeInt(0); break;
                    default: throw new RuntimeException ("Cannot serialize graph: no serialization support for the vertex neighbourhood class " + className);
                }
                for (int i=0; i < size; i++) {
                    if (vertexNeighbourhoods[i] == null){
                        out.writeBoolean(true);
                    } else {
                        out.writeBoolean(false);
                        vertexNeighbourhoods[i].write(out);
                    }
                }
            }
        }

        if (vertexIndexF == null){
            out.writeInt(-1);
        } else {
            int size = vertexIndexF.length;
            out.writeInt(size);
            for (int i=0; i < size; i++){
                if (vertexIndexF[i] == null){
                    out.writeBoolean(true);
                } else {
                    out.writeBoolean(false);
                    vertexIndexF[i].write(out);
                }
            }
        }

        if (edgeIndexF == null){
            out.writeInt(-1);
        } else {
            int size = edgeIndexF.length;
            out.writeInt(size);
            for (int i=0; i < size; i++){
                if(edgeIndexF[i] == null){
                    out.writeBoolean(true);
                } else {
                    out.writeBoolean(false);
                    edgeIndexF[i].write(out);
                }
            }
        }

        out.writeBoolean(negative_edge_label);
    }

    public void read (ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.read(in);

        System.out.println("BasicMainGraph.read");

        vertexNeighbourhoods = null;
        int size = in.readInt();
        if (size >= 0){
            if (size > 0){
                vertexNeighbourhoods = new VertexNeighbourhood[size];
                int classType = in.readInt();
                for (int i=0; i < size; i++) {
                    boolean isNull = in.readBoolean();
                    if (!isNull) {
                        switch (classType){
                            case 0 : vertexNeighbourhoods[i] = new BasicVertexNeighbourhood(); break;
                            default: throw new RuntimeException ("Cannot deserialize graph: no serialization support for the vertex neighbourhood class " + classType);
                        }
                        vertexNeighbourhoods[i].read(in);
                    }
                }
            }
        }

        vertexIndexF = null;
        size = in.readInt();
        if (size >= 0){
            vertexIndexF = new Vertex[size];
            for (int i=0; i < size; i++){
                boolean isNull = in.readBoolean();
                if (!isNull) {
                    vertexIndexF[i] = new Vertex();
                    vertexIndexF[i].readFields(in);
                }
            }
        }

        edgeIndexF = null;
        size = in.readInt();
        if (size >= 0){
            edgeIndexF = new Edge[size];
            for (int i=0; i < size; i++){
                boolean isNull = in.readBoolean();
                if (!isNull) {
                    edgeIndexF[i] = new Edge();
                    edgeIndexF[i].readFields(in);
                }
            }
        }

        negative_edge_label = in.readBoolean();
    }
    //***** End of Modifications coming from QFrag
}
