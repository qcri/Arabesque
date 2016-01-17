package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.function.IntConsumer;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.StringTokenizer;

public class BasicMainGraph implements MainGraph {
    private static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

    private static final int INITIAL_ARRAY_SIZE = 4096;

    private Vertex[] vertexIndexF;
    private Edge[] edgeIndexF;

    private int numVertices;
    private int numEdges;

    private VertexNeighbourhood[] vertexNeighbourhoods;

    private boolean isEdgeLabelled;
    private boolean isMultiGraph;
    private String name;

    private void init(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
        this.name = name;
        long start = 0;

        if (LOG.isInfoEnabled()) {
            start = System.currentTimeMillis();
            LOG.info("Initializing");
        }

        vertexIndexF = null;
        edgeIndexF = null;

        vertexNeighbourhoods = null;

        reset();

        this.isEdgeLabelled = isEdgeLabelled;
        this.isMultiGraph = isMultiGraph;

        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
        }
    }

    private void init(Object path) throws IOException {
        long start = 0;

        if (LOG.isInfoEnabled()) {
            LOG.info("Reading graph");
            start = System.currentTimeMillis();
        }

        if (path instanceof Path) {
            Path filePath = (Path) path;
            readFromFile(filePath);
        } else if (path instanceof org.apache.hadoop.fs.Path) {
            org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
            readFromHdfs(hadoopPath);
        } else {
            throw new RuntimeException("Invalid path: " + path);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }
    }

    private void prepareStructures(int numVertices, int numEdges) {
        ensureCanStoreNewVertices(numVertices);
        ensureCanStoreNewEdges(numEdges);
    }

    @Override
    public void reset() {
        numVertices = 0;
        numEdges = 0;
    }

    private void ensureCanStoreNewVertices(int numVerticesToAdd) {
        int newMaxVertexId = numVertices + numVerticesToAdd;

        ensureCanStoreUpToVertex(newMaxVertexId);
    }

    private void ensureCanStoreUpToVertex(int maxVertexId) {
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

    private void ensureCanStoreNewVertex() {
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

    private void ensureCanStoreNewEdge() {
        ensureCanStoreNewEdges(1);
    }

    public BasicMainGraph(String name) {
        this(name, false, false);
    }

    public BasicMainGraph(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
        init(name, isEdgeLabelled, isMultiGraph);
    }

    public BasicMainGraph(Path filePath, boolean isEdgeLabelled, boolean isMultiGraph)
            throws IOException {
        this(filePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
        init(filePath);
    }

    public BasicMainGraph(org.apache.hadoop.fs.Path hdfsPath, boolean isEdgeLabelled, boolean isMultiGraph)
            throws IOException {
        this(hdfsPath.getName(), isEdgeLabelled, isMultiGraph);
        init(hdfsPath);
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
        vertexIndexF[numVertices++] = vertex;

        return this;
    }

    @Override
    public Vertex[] getVertices() {
        return vertexIndexF;
    }

    @Override
    public Vertex getVertex(int vertexId) {
        return vertexIndexF[vertexId];
    }

    @Override
    public int getNumberVertices() {
        return numVertices;
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
    public int getNumberEdges() {
        return numEdges;
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
            edge.setEdgeId(numEdges);
        } else if (edge.getEdgeId() != numEdges) {
            throw new RuntimeException("Sanity check, edge with id " + edge.getEdgeId() + " added at position " + numEdges);
        }

        ensureCanStoreNewEdge();
        ensureCanStoreUpToVertex(Math.max(edge.getSourceId(), edge.getDestinationId()));
        edgeIndexF[numEdges++] = edge;

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

        return this;
    }

    protected void readFromHdfs(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        InputStream is = fs.open(hdfsPath);
        readFromInputStream(is);
        is.close();
    }

    protected void readFromFile(Path filePath) throws IOException {
        InputStream is = Files.newInputStream(filePath);
        readFromInputStream(is);
        is.close();
    }

    protected void readFromInputStream(InputStream is) {
        int prev_vertex_id=-1;
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
                if (prev_vertex_id+1 != vertex.getVertexId()){
                    throw new RuntimeException("Input graph isn't sorted by vertex id, or vertex id not sequential\n " +
                                               "Expecting:"+(prev_vertex_id+1) + " Found:"+vertex.getVertexId());
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
    }

    protected Edge parseEdge(StringTokenizer tokenizer, int vertexId) {
        int neighborId = Integer.parseInt(tokenizer.nextToken());

        if (isEdgeLabelled) {
            int edgeLabel = Integer.parseInt(tokenizer.nextToken());
            return createEdge(vertexId, neighborId, edgeLabel);
        }
        else {
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
}
