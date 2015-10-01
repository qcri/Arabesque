package io.arabesque.pattern;

import io.arabesque.embedding.Embedding;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import org.apache.hadoop.io.Writable;

public abstract class Pattern implements Writable {
    public abstract Pattern copy();

    public abstract void reset();

    public abstract void setEmbedding(Embedding embedding);

    public abstract int getNumberOfVertices();

    public abstract boolean addEdge(int edgeId);

    public abstract boolean addEdge(PatternEdge patternEdge);

    public abstract int getNumberOfEdges();

    public abstract void generateMinPatternCode();

    public abstract int[] getVertices();

    public abstract PatternEdge[] getPatternEdges();

    public abstract HashIntSet[] getAutoVertexSet();

    public boolean isSubPattern(Pattern pattern) {
        if (this.getNumberOfEdges() > pattern.getNumberOfEdges()) return false;
        if (this.getNumberOfVertices() > pattern.getNumberOfVertices()) return false;

        int numberOfEdges = getNumberOfEdges();
        PatternEdge[] myEdges = getPatternEdges();
        PatternEdge[] otherEdges = pattern.getPatternEdges();

        for (int i = 0; i < numberOfEdges; ++i) {
            if (!myEdges[i].equals(otherEdges[i])) {
                return false;
            }
        }

        return true;
    }

    public abstract HashIntIntMap getCanonicalLabeling();

    public abstract PatternEdge[] getEdges();
}
