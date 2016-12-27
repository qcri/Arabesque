package io.arabesque.pattern;

import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.collect.map.IntIntMap;
import org.apache.hadoop.io.Writable;

import java.io.Externalizable;

public interface Pattern extends Writable, Externalizable {
    Pattern copy();

    void reset();

    void setEmbedding(Embedding embedding);

    int getNumberOfVertices();

    boolean addEdge(int edgeId);

    boolean addEdge(PatternEdge patternEdge);

    int getNumberOfEdges();

    boolean turnCanonical();

    IntArrayList getVertices();

    PatternEdgeArrayList getEdges();

    VertexPositionEquivalences getVertexPositionEquivalences();

    IntIntMap getCanonicalLabeling();

    String toOutputString();
   
    ////////
    boolean equals(Object o, int upTo);
    
}
