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
import java.util.StringTokenizer;

/**
 * Created by siganos on 3/9/16.
 */
public class BasicMainGraph extends AbstractMainGraph {
  private static final Logger LOG = Logger.getLogger(BasicMainGraph.class);

  private static final int INITIAL_ARRAY_SIZE = 4096;
  protected VertexNeighbourhood[] vertexNeighbourhoods;
  private Vertex[] vertexIndexF;
  Edge[] edgeIndexF;
  protected boolean negative_edge_label; //Used for search.

  public BasicMainGraph(String name) {
    super(name);
  }

  public BasicMainGraph(String name, boolean a, boolean b) {
    super(name, a, b);
  }

  public BasicMainGraph(Path filePath) throws IOException {
    super(filePath);
  }

  public BasicMainGraph(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
    super(hdfsPath);
  }
  @Override
  public void reset() {
    numVertices = 0;
    numEdges = 0;
    vertexIndexF = null;
    edgeIndexF = null;
    vertexNeighbourhoods = null;

  }

  @Override
  public int getVertexLabel(int v) {
    return vertexIndexF[v].getVertexLabel();
  }

  protected void prepareStructures(int numVertices, int numEdges) {
    ensureCanStoreNewVertices(numVertices);
    ensureCanStoreNewEdges(numEdges);
  }

  public IntCollection getVertexNeighbors(int vertexId) {
    if(vertexNeighbourhoods[vertexId]!=null){
      return vertexNeighbourhoods[vertexId].getNeighbourVertices();
    }
    return null;
  }

  private void ensureCanStoreNewVertices(int numVerticesToAdd) {
    int newMaxVertexId = (int) (numVertices + numVerticesToAdd);

    ensureCanStoreUpToVertex(newMaxVertexId);
  }

  void ensureCanStoreUpToVertex(int maxVertexId) {
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

    return this;
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
      //halt if vertex self loop is found
      if( edge.getSourceId() == edge.getDestinationId()) {
        LOG.error("The input graph contains vertices having self loops. Arabesque does not support self loops");
        LOG.error("Self loop at vertex #: " + edge.getSourceId());
        throw new RuntimeException("Graphs with self loops are not supported");
      }

      edgeIndexF[(int) numEdges] = edge;
      addToNeighborhood(edge);
      numEdges++;
    }

    // Dump vertexIndexF create (empty). Marco should probably need this.
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

  protected void readFromInputStreamText(InputStream is) {
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
    if (LOG.isInfoEnabled()) {
      LOG.info("Done in " + (System.currentTimeMillis() - start));
      LOG.info("Number vertices: " + numVertices);
      LOG.info("Number edges: " + numEdges);
    }
    //System.out.println("Finish reading:"+negative_edge_label);
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
      //System.out.println("Adding edge:"+vertexId+" "+neighborId);
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

  protected Vertex createVertex(int id, int label) {
    return new Vertex(id, label);
  }

  protected Edge createEdge(int srcId, int destId) {
    return new Edge(srcId, destId);
  }

  protected Edge createEdge(int srcId, int destId, int label) {
    return new LabelledEdge(srcId, destId, label);
  }

  protected Edge createEdge(int edgeId, int srcId, int destId, int label) {
    return new LabelledEdge(edgeId, srcId, destId, label);
  }

  protected Edge createEdge(int srcId, int destId, float label) {
    return new FloatEdge(srcId, destId, label);
  }


  private VertexNeighbourhood createVertexNeighbourhood() {
    if (!isMultiGraph) {
      return new BasicVertexNeighbourhood();
    } else {
      return new MultiVertexNeighbourhood();
    }
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

  //Used only with the search Graph. The following functions aren't in the MainGraph API.

  public IntCollection getVertexNeighbours(int vertexId) {

    if (vertexNeighbourhoods[vertexId] == null) {
      return null;
    }

    return vertexNeighbourhoods[vertexId].getNeighbourVertices();
  }

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
}
