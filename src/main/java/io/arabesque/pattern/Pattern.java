package io.arabesque.pattern;

import io.arabesque.embedding.Embedding;
import io.arabesque.utils.IntArrayList;
import net.openhft.koloboke.collect.map.IntIntMap;
import org.apache.hadoop.io.Writable;

public abstract class Pattern implements Writable {
    public abstract Pattern copy();

    public abstract void reset();

    public abstract void setEmbedding(Embedding embedding);

    public abstract int getNumberOfVertices();

    public abstract boolean addEdge(int edgeId);

    public abstract boolean addEdge(PatternEdge patternEdge);

    public abstract int getNumberOfEdges();

    public abstract void turnCanonical();

    public abstract IntArrayList getVertices();

    public abstract PatternEdgeArrayList getEdges();

    public abstract VertexPositionEquivalences getVertexPositionEquivalences();

    public boolean isSubPattern(Pattern pattern) {
        if (this.getNumberOfEdges() > pattern.getNumberOfEdges()) return false;
        if (this.getNumberOfVertices() > pattern.getNumberOfVertices()) return false;

        int numberOfEdges = getNumberOfEdges();
        PatternEdgeArrayList myEdges = getEdges();
        PatternEdgeArrayList otherEdges = pattern.getEdges();

        for (int i = 0; i < numberOfEdges; ++i) {
            if (!myEdges.get(i).equals(otherEdges.get(i))) {
                return false;
            }
        }

        return true;
    }

    public abstract IntIntMap getCanonicalLabeling();

    public abstract String toOutputString();
}
