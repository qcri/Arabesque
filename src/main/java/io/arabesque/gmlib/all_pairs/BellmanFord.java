package io.arabesque.examples.all_pairs;

/**
 * Created by siganos on 2/16/16.
 */

import io.arabesque.graph.UnsafeCSRMainGraphEdgeLabelFloat;

import java.lang.reflect.Field;

public class BellmanFord  {
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

  private final static long FLOAT_SIZE_IN_BYTES = 4;
  private final int numberVertices;
  private long distances;

  BellmanFord(int numberVertices) {
    distances = UNSAFE.allocateMemory(numberVertices * FLOAT_SIZE_IN_BYTES);
    this.numberVertices = numberVertices;
  }

  private void sD(long index, float value) {
    UNSAFE.putFloat(distances + (index*FLOAT_SIZE_IN_BYTES), value);
  }

  private float gD(long index){
    return UNSAFE.getFloat(distances+(index*FLOAT_SIZE_IN_BYTES));
  }

  /**
   * GRAPH IS UNDIRECTED, SO NEED TO DO FEW TRICKS.
   * @param graph
   * @param source
     */
  void relax(UnsafeCSRMainGraphEdgeLabelFloat graph, int source) {
    int i,j;

    //Clear the structure.
    UNSAFE.setMemory(distances,numberVertices * FLOAT_SIZE_IN_BYTES, (byte) 0);

    boolean changed;
    sD(source,1.0f);
    final int num_edges = graph.getNumberEdges();
    final int num_vertices = graph.getNumberVertices();
    for (i = 0; i < num_vertices; ++i) {
      changed = false;

      for (j = 0; j < num_edges; ++j) {
        final int e_src   = graph.getEdgeSource(j);
        final float e_label = graph.getEdgeLabelFloat(j);
        final int e_dst   = graph.getEdgeDst(j);

        if (gD(e_src) * e_label > gD(e_dst)) {
          changed = true;
          sD(e_dst, gD(e_src) * e_label);
        }

        // Next, do the reverse direction also. We have undirected graphs
        // and source is always smaller than the destination.
        if (gD(e_dst) * e_label > gD(e_src)) {
          changed = true;
          sD(e_src,gD(e_dst) * e_label);
        }
      }
      if (!changed){
//        System.out.println("Breaking early:"+i);
        break;
      }
    }
  }

  String print_array(int source) {
      StringBuilder a = new StringBuilder();

      for (int i=0;i < source;i++){
        if (gD(i)>0.1f){
            a.append(source).append(" ").append(i).append(" ").append(gD(i)).append("\n");
        }
      }
      //Last element is different for avoiding adding the \n
//      int i = numberVertices-1;
//      if (gD(i)>0.1f && source < i){
//        a.append(source).append(" ").append(i).append(" ").append(gD(i));
//      }
      return a.toString();
    }
  }

/*
  boolean cycle() {
    int j;
    for (j = 0; j < e; ++j)
      if (d[edges.get(j).u] + edges.get(j).w < d[edges.get(j).v])
        return false;
    return true;
  }


  void print() {
    for (int i = 0; i < n; i++) {
      System.out.println("Vertex " + i + " has predecessor " + p[i]);
    }
  }
  */