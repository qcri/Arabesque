package io.arabesque.pattern;

import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.map.IntIntMap;
import org.apache.hadoop.io.Writable;

public interface Pattern extends Writable {
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
}
