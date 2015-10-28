package io.arabesque.graph;

import io.arabesque.utils.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMapFactory;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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

public abstract class BasicMainGraph<
        VD extends Writable,
        QV extends Vertex<VD>,
        EL extends WritableComparable,
        QE extends Edge<EL>> implements MainGraph<VD,QV,EL,QE> {
    private static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

    private static final int INITIAL_ARRAY_SIZE = 4096;

    private Vertex<VD>[] vertexIndexF;
    private Edge<EL>[] edgeIndexF;

    private int numVertices;
    private int numEdges;

    private HashIntIntMapFactory intIntMapFactory = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);

    private HashIntIntMap[] vertexPairToEdge;

    private IntArrayList[] edgeNeighbourhoodIndex;
    private IntArrayList[] vertexNeighbourhoodIndex;

    private static final IntArrayList EMPTY_INT_ARRAY_LIST = new IntArrayList(0);
    private static final IntIntMap EMPTY_INT_INT_MAP = HashIntIntMaps.newImmutableMap(new HashMap<Integer, Integer>());

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

        edgeNeighbourhoodIndex = null;
        vertexNeighbourhoodIndex = null;

        reset();

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
        if (vertexIndexF == null) {
            vertexIndexF = new Vertex[Math.max(numVerticesToAdd, INITIAL_ARRAY_SIZE)];
        } else if (vertexIndexF.length < numVertices + numVerticesToAdd) {
            vertexIndexF = Arrays.copyOf(vertexIndexF, (vertexIndexF.length + numVerticesToAdd) << 1);
        }

        if (vertexPairToEdge == null) {
            vertexPairToEdge = new HashIntIntMap[Math.max(numVerticesToAdd, INITIAL_ARRAY_SIZE)];
        } else if (vertexPairToEdge.length < numVertices + numVerticesToAdd) {
            vertexPairToEdge = Arrays.copyOf(vertexPairToEdge, (vertexPairToEdge.length + numVerticesToAdd) << 1);
        }
    }

    private void ensureCanStoreUpToVertex(int maxVertexId) {
        int delta = maxVertexId - vertexIndexF.length;

        if (delta < 0) {
            return;
        }

        ensureCanStoreNewVertices(delta + 1);
    }

    private void ensureCanStoreNewVertex() {
        ensureCanStoreNewVertices(1);
    }

    private void ensureCanStoreNewEdges(int numEdgesToAdd) {
        if (edgeIndexF == null) {
            edgeIndexF = new Edge[Math.max(numEdgesToAdd, INITIAL_ARRAY_SIZE)];
        } else if (edgeIndexF.length < numEdges + numEdgesToAdd) {
            edgeIndexF = Arrays.copyOf(edgeIndexF, (edgeIndexF.length + numEdgesToAdd) << 1);
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
    public MainGraph<VD, QV, EL, QE> addVertex(QV vertex) {
        ensureCanStoreNewVertex();
        vertexIndexF[numVertices++] = vertex;

        return this;
    }

    @Override
    public QV[] getVertices() {
        return (QV[]) vertexIndexF;
    }

    @Override
    public QV getVertex(int vertexId) {
        return (QV) vertexIndexF[vertexId];
    }

    @Override
    public int getNumberVertices() {
        return numVertices;
    }

    @Override
    public QE[] getEdges() {
        return (QE[]) edgeIndexF;
    }

    @Override
    public QE getEdge(int edgeId) {
        return (QE) edgeIndexF[edgeId];
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
    public MainGraph<VD, QV, EL, QE> addEdge(QE edge) {
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
            throw new ArrayIndexOutOfBoundsException("Tried to access index " + edge.getSourceId() + " of array with size " + vertexPairToEdge.length);
        }

        try {
            HashIntIntMap dstConnections = vertexPairToEdge[edge.getDestinationId()];

            if (dstConnections == null) {
                dstConnections = intIntMapFactory.newMutableMap();
                vertexPairToEdge[edge.getDestinationId()] = dstConnections;
            }

            dstConnections.put(edge.getSourceId(), edge.getEdgeId());
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new ArrayIndexOutOfBoundsException("Tried to access index " + edge.getDestinationId() + " of array with size " + vertexPairToEdge.length);
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
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));

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

                int vertexId = Integer.parseInt(tokenizer.nextToken());
                int vertexLabel = Integer.parseInt(tokenizer.nextToken());
                VD vertexValue = readVertexData(tokenizer);

                QV vertex = createVertex(vertexId, vertexLabel, vertexValue);

                addVertex(vertex);

                while (tokenizer.hasMoreTokens()) {
                    int neighborId = Integer.parseInt(tokenizer.nextToken());
                    QE edge = createEdge(vertexId, neighborId);
                    addEdge(edge);
                }

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "Vertices: " + Arrays.toString(vertexIndexF) + "\n Edges: " + Arrays.toString(edgeIndexF);
    }

    @Override
    public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
        Edge<EL> edge1 = edgeIndexF[edge1Id];
        Edge<EL> edge2 = edgeIndexF[edge2Id];

        return edge1.neighborWith(edge2);
    }

    @Override
    public boolean isNeighborEdge(int src1, int dest1, int edge2) {
        int src2 = edgeIndexF[edge2].getSourceId();

        if (src1 == src2) return true;

        int dest2 = edgeIndexF[edge2].getDestinationId();

        return (dest1 == src2 || dest1 == dest2 || src1 == dest2);
    }

    protected abstract VD readVertexData(StringTokenizer tokenizer);

    protected abstract QV createVertex();

    protected abstract QV createVertex(int id, int label, VD data);

    protected abstract QE createEdge();

    protected abstract QE createEdge(int srcId, int destId);

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
