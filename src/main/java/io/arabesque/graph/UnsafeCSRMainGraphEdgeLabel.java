package io.arabesque.graph;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.StringTokenizer;

/**
 * TODO: MAKE THIS MULTIGRAPH...
 * This is not multigraph because the getEdgeLabel then should
 * return a set, not a single label.
 * Created by siganos on 3/9/16.
 */
public class UnsafeCSRMainGraphEdgeLabel extends UnsafeCSRMainGraph {
  private static final Logger LOG = Logger.getLogger(UnsafeCSRMainGraphEdgeLabel.class);

  private long labelIndex;

  public UnsafeCSRMainGraphEdgeLabel(String name) {
    super(name);
  }

  public UnsafeCSRMainGraphEdgeLabel(String name, boolean a, boolean b) {
    super(name, a, b);
  }

  public UnsafeCSRMainGraphEdgeLabel(Path filePath) throws IOException {
    super(filePath);
  }

  public UnsafeCSRMainGraphEdgeLabel(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
    super(hdfsPath);
  }

  @Override
  public void build() {
    super.build();
    labelIndex = UNSAFE.allocateMemory(numEdges * INT_SIZE_IN_BYTES);
  }

  @Override
  public int getEdgeLabel(int index) {
    return UNSAFE.getInt(labelIndex+(index*INT_SIZE_IN_BYTES));
  }

  public void setEdgeLabel(int index, int value) {
    UNSAFE.putInt(labelIndex+(index*INT_SIZE_IN_BYTES),value);
  }

  @Override
  public boolean isEdgeLabelled() {
    return true;
  }

  @Override
  protected int parse_edge(StringTokenizer tokenizer, int vertexId, int edges_position) {
    int prev_edge = -1;

    while (tokenizer.hasMoreTokens()) {
      int neighborId = Integer.parseInt(tokenizer.nextToken());

      if (prev_edge >= 0 && prev_edge > neighborId) {
        throw new RuntimeException("The edges need to be sorted for unsafe");
      }

      int edgeLabel = Integer.parseInt(tokenizer.nextToken());

      prev_edge = neighborId;
      setEdgeSource(edges_position, vertexId);
      setEdgeDst(edges_position, neighborId);
      setEdgeLabel(edges_position,edgeLabel);
      edges_position++;
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
    int prev_neighbor = -1;

    while (in.available() > 0) {
      int c_v_id = in.readInt();
      int neigbor = in.readInt();
      int label = in.readInt();

      if (c_v_id!=vertex_id){
        //Sanity for correct input.
        if (vertex_id+1!=c_v_id){
          throw new RuntimeException("Vertices should be strictly incremental");
        }
        //Add the pos.
        setVertexPos(c_v_id,edge_pos);
        vertex_id = c_v_id;
        prev_neighbor = -1;
      }

      //Sanity for correct input.
      if (prev_neighbor > neigbor){
        throw new RuntimeException("Edges should be ordered by increasing id");
      }

      setEdgeSource(edge_pos,vertex_id);
      setEdgeDst(edge_pos,neigbor);
      setEdgeLabel(edge_pos,label);

      prev_neighbor = neigbor;
      edge_pos++;

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

}
