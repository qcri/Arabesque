package io.arabesque.optimization;

import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface OrderedNeighboursMainGraph<VD extends Writable, QV extends Vertex<VD>, EL extends WritableComparable, QE extends Edge<EL>> extends MainGraph<VD, QV, EL, QE> {
}
