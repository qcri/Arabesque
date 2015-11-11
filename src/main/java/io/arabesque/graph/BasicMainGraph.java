package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import io.arabesque.utils.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMapFactory;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
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
import java.util.HashMap;
import java.util.StringTokenizer;

public class BasicMainGraph implements MainGraph {
    private static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

    private static final int INITIAL_ARRAY_SIZE = 4096;

    private Vertex[] vertexIndexF;
    private Edge[] edgeIndexF;

    private int numVertices;
    private int numEdges;

    private HashIntIntMapFactory intIntMapFactory = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);

    private HashIntIntMap[] vertexPairToEdge;

    private IntArrayList[] vertexNeighbourhoodIndex;

    private static final IntArrayList EMPTY_INT_ARRAY_LIST = new IntArrayList(0);
    private static final IntIntMap EMPTY_INT_INT_MAP = HashIntIntMaps.newImmutableMap(new HashMap<Integer, Integer>());

    private boolean edgeLabelled;

    private void init(Object path) throws IOException {
        long start = 0;
        long totalStart = 0;

        if (LOG.isInfoEnabled()) {
            totalStart = start = System.currentTimeMillis();
            LOG.info("Initializing");
        }

        vertexIndexF = null;
        edgeIndexF = null;

        vertexPairToEdge = null;

        vertexNeighbourhoodIndex = null;

        reset();

        Configuration conf = Configuration.get();

        edgeLabelled = conf.isGraphEdgeLabelled();

        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
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
            LOG.info("Finished in " + (System.currentTimeMillis() - totalStart) + " miliseconds");
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }
    }

    private void constructSortedVertexNeighbourhoodIndex() {
        vertexNeighbourhoodIndex = new IntArrayList[numVertices];

        for (int i = 0; i < numVertices; ++i) {
            HashIntIntMap vertexConnections = vertexPairToEdge[i];

            if (vertexConnections == null) {
                vertexNeighbourhoodIndex[i] = EMPTY_INT_ARRAY_LIST;
                continue;
            }

            IntArrayList vertexNeighbourhoodEntry = new IntArrayList(vertexConnections.size());
            vertexNeighbourhoodIndex[i] = vertexNeighbourhoodEntry;

            IntIntCursor vertexConnectionsCursor = vertexConnections.cursor();
            while (vertexConnectionsCursor.moveNext()) {
                vertexNeighbourhoodEntry.add(vertexConnectionsCursor.key());
            }

            vertexNeighbourhoodEntry.sort();
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

        if (vertexPairToEdge == null) {
            vertexPairToEdge = new HashIntIntMap[Math.max(targetSize, INITIAL_ARRAY_SIZE)];
        } else if (vertexPairToEdge.length < targetSize) {
            vertexPairToEdge = Arrays.copyOf(vertexPairToEdge, getSizeWithPaddingWithoutOverflow(targetSize, vertexPairToEdge.length));
        }
    }

    private int getSizeWithPaddingWithoutOverflow(int targetSize, int currentSize) {
        if (currentSize > targetSize) {
            return currentSize;
        }

        int sizeWithPadding = targetSize + ((targetSize - currentSize) << 1);

        // If we saw an overflow, return simple targetSize
        if (sizeWithPadding < targetSize) {
            return targetSize;
        }
        else {
            return sizeWithPadding;
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

    public BasicMainGraph(Path filePath)
            throws IOException {
        init(filePath);
    }

    public BasicMainGraph(org.apache.hadoop.fs.Path hdfsPath)
            throws IOException {
        init(hdfsPath);
    }

    @Override
    public boolean isNeighborVertex(int v1, int v2) {
        HashIntIntMap v1Connections = vertexPairToEdge[v1];

        if (v1Connections != null) {
            return v1Connections.containsKey(v2);
        } else {
            return false;
        }
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
    public int getEdgeId(int v1, int v2) {
        int minv;
        int maxv;
        if (v1 < v2) {
            minv = v1;
            maxv = v2;
        } else {
            minv = v2;
            maxv = v1;
        }

        HashIntIntMap minConnections = vertexPairToEdge[minv];

        if (minConnections == null) {
            return -1;
        }

        return minConnections.getOrDefault(maxv, -1);
    }

    @Override
    public MainGraph addEdge(Edge edge) {
        // Assuming input graph contains all edges but we treat it as undirected
        // TODO: What if input only contains one of the edges? Should we enforce
        // this via a sanity check?
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
            HashIntIntMap srcConnections = vertexPairToEdge[edge.getSourceId()];

            if (srcConnections == null) {
                srcConnections = intIntMapFactory.newMutableMap();
                vertexPairToEdge[edge.getSourceId()] = srcConnections;
            }

            srcConnections.put(edge.getDestinationId(), edge.getEdgeId());
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Tried to access index " + edge.getSourceId() + " of array with size " + vertexPairToEdge.length);
            LOG.error("vertexIndexF.length=" + vertexIndexF.length);
            LOG.error("vertexPairToEdge.length=" + vertexPairToEdge.length);
            throw e;
        }

        try {
            HashIntIntMap dstConnections = vertexPairToEdge[edge.getDestinationId()];

            if (dstConnections == null) {
                dstConnections = intIntMapFactory.newMutableMap();
                vertexPairToEdge[edge.getDestinationId()] = dstConnections;
            }

            dstConnections.put(edge.getSourceId(), edge.getEdgeId());
        } catch (ArrayIndexOutOfBoundsException e) {
            LOG.error("Tried to access index " + edge.getDestinationId() + " of array with size " + vertexPairToEdge.length);
            LOG.error("vertexIndexF.length=" + vertexIndexF.length);
            LOG.error("vertexPairToEdge.length=" + vertexPairToEdge.length);
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

                int vertexId = parseVertex(tokenizer);

                while (tokenizer.hasMoreTokens()) {
                    parseEdge(tokenizer, vertexId);
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void parseEdge(StringTokenizer tokenizer, int vertexId) {
        int neighborId = Integer.parseInt(tokenizer.nextToken());

        Edge edge;

        if (!edgeLabelled) {
            edge = createEdge(vertexId, neighborId);
        }
        else {
            int edgeLabel = Integer.parseInt(tokenizer.nextToken());

            edge = createEdge(vertexId, neighborId, edgeLabel);
        }

        addEdge(edge);
    }

    protected int parseVertex(StringTokenizer tokenizer) {
        int vertexId = Integer.parseInt(tokenizer.nextToken());
        int vertexLabel = Integer.parseInt(tokenizer.nextToken());

        Vertex vertex = createVertex(vertexId, vertexLabel);

        addVertex(vertex);

        return vertexId;
    }

    @Override
    public String toString() {
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

    @Override
    public IntIntMap getVertexNeighbourhood(int vertexId) {
        return vertexPairToEdge[vertexId];
    }

    @Override
    public IntCollection getVertexNeighbours(int vertexId) {
        IntIntMap neighbourhoodMap = getVertexNeighbourhood(vertexId);

        if (neighbourhoodMap == null) {
            return null;
        }

        return neighbourhoodMap.keySet();
    }

}
