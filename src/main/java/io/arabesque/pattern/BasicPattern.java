package io.arabesque.pattern;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.utils.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMapFactory;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public abstract class BasicPattern extends Pattern {
    private static final Logger LOG = Logger.getLogger(BasicPattern.class);

    protected HashIntIntMapFactory positionMapFactory = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);

    // Basic structure {{
    private IntArrayList vertices;
    private PatternEdgeArrayList edges;
    // K = vertex id, V = vertex position
    private IntIntMap vertexPositions;
    // }}

    // Incremental building {{
    private IntArrayList previousWords;
    private int numVerticesAddedFromPrevious;
    private int numAddedEdgesFromPrevious;
    // }}

    // Isomorphisms {{
    private VertexPositionEquivalences vertexPositionEquivalences;
    private IntIntMap canonicalLabelling;
    // }}

    // Others {{
    protected final MainGraph<?, ?, ?, ?> mainGraph;
    private PatternEdgeArrayList patternEdgeListPool;

    protected volatile boolean dirtyVertexPositionEquivalences;
    protected volatile boolean dirtyCanonicalLabelling;
    // }}


    public BasicPattern() {
        mainGraph = Configuration.get().getMainGraph();

        vertices = new IntArrayList();
        edges = createPatternEdgeArrayList();
        vertexPositions = positionMapFactory.newMutableMap();
        previousWords = new IntArrayList();
        patternEdgeListPool = null;

        init();
    }

    public BasicPattern(BasicPattern basicPattern) {
        this();

        vertices.addAll(basicPattern.vertices);

        edges.ensureCapacity(basicPattern.edges.size());

        for (PatternEdge otherEdge : basicPattern.edges) {
            edges.add(new PatternEdge(otherEdge));
        }

        vertexPositions.putAll(basicPattern.vertexPositions);
    }

    protected void init() {
        reset();
    }

    @Override
    public void reset() {
        vertices.clear();
        reclaimPatternEdges(edges);
        edges.clear();
        vertexPositions.clear();

        setDirty();

        resetIncremental();
    }

    private void resetIncremental() {
        numVerticesAddedFromPrevious = 0;
        numAddedEdgesFromPrevious = 0;
        previousWords.clear();
    }

    @Override
    public void setEmbedding(Embedding embedding) {
        if (canDoIncremental(embedding)) {
            setEmbeddingIncremental(embedding);
        } else {
            setEmbeddingFromScratch(embedding);
        }
    }

    /**
     * Reset everything and do everything from scratch.
     *
     * @param embedding
     */
    private void setEmbeddingFromScratch(Embedding embedding) {
        reset();

        int numEdgesInEmbedding = embedding.getNumEdges();
        int numVerticesInEmbedding = embedding.getNumVertices();

        if (numEdgesInEmbedding == 0 && numVerticesInEmbedding == 0) {
            return;
        }

        ensureCanStoreNewVertices(numVerticesInEmbedding);
        ensureCanStoreNewEdges(numEdgesInEmbedding);

        int[] embeddingVertices = embedding.getVertices();

        for (int i = 0; i < numVerticesInEmbedding; ++i) {
            addVertex(embeddingVertices[i]);
        }

        numVerticesAddedFromPrevious = embedding.getNumVerticesAddedWithExpansion();

        int[] embeddingEdges = embedding.getEdges();

        for (int i = 0; i < numEdgesInEmbedding; ++i) {
            addEdge(embeddingEdges[i]);
        }

        numAddedEdgesFromPrevious = embedding.getNumEdgesAddedWithExpansion();

        updateUsedEmbeddingFromScratch(embedding);
    }

    private void updateUsedEmbeddingFromScratch(Embedding embedding) {
        previousWords.clear();

        int embeddingNumWords = embedding.getNumWords();

        previousWords.ensureCapacity(embeddingNumWords);

        int[] words = embedding.getWords();

        for (int i = 0; i < embeddingNumWords; i++) {
            previousWords.add(words[i]);
        }
    }

    private void resetToPrevious() {
        removeLastNEdges(numAddedEdgesFromPrevious);
        removeLastNVertices(numVerticesAddedFromPrevious);
        setDirty();
    }

    private void removeLastNEdges(int n) {
        int targetI = edges.size() - n;

        for (int i = edges.size() - 1; i >= targetI; --i) {
            reclaimPatternEdge(edges.remove(i));
        }
    }

    private void removeLastNVertices(int n) {
        int targetI = vertices.size() - n;

        for (int i = vertices.size() - 1; i >= targetI; --i) {
            try {
                vertexPositions.remove(vertices.remove(i));
            } catch (IllegalArgumentException e) {
                System.err.println(e.toString());
                System.err.println("i=" + i);
                System.err.println("targetI=" + targetI);
                throw e;
            }
        }
    }

    /**
     * Only the last word has changed, so skipped processing for the previous words.
     *
     * @param embedding
     */
    private void setEmbeddingIncremental(Embedding embedding) {
        resetToPrevious();

        numVerticesAddedFromPrevious = embedding.getNumVerticesAddedWithExpansion();
        numAddedEdgesFromPrevious = embedding.getNumEdgesAddedWithExpansion();

        int numEdgesInEmbedding = embedding.getNumEdges();
        int numVerticesInEmbedding = embedding.getNumVertices();

        ensureCanStoreNewVertices(numVerticesAddedFromPrevious);
        ensureCanStoreNewEdges(numAddedEdgesFromPrevious);

        int[] embeddingVertices = embedding.getVertices();

        for (int i = (numVerticesInEmbedding - numVerticesAddedFromPrevious); i < numVerticesInEmbedding; ++i) {
            addVertex(embeddingVertices[i]);
        }

        int[] embeddingEdges = embedding.getEdges();

        for (int i = (numEdgesInEmbedding - numAddedEdgesFromPrevious); i < numEdgesInEmbedding; ++i) {
            addEdgeWithPositions(embeddingEdges[i]);
        }

        updateUsedEmbeddingIncremental(embedding);
    }

    /**
     * By default only the last word changed.
     *
     * @param embedding
     */
    private void updateUsedEmbeddingIncremental(Embedding embedding) {
        previousWords.set(previousWords.size() - 1, embedding.getWords()[previousWords.size() - 1]);
    }

    private void ensureCanStoreNewEdges(int numAddedEdgesFromPrevious) {
        int newNumEdges = edges.size() + numAddedEdgesFromPrevious;

        edges.ensureCapacity(newNumEdges);
    }

    private void ensureCanStoreNewVertices(int numVerticesAddedFromPrevious) {
        int newNumVertices = vertices.size() + numVerticesAddedFromPrevious;

        vertices.ensureCapacity(newNumVertices);
        vertexPositions.ensureCapacity(newNumVertices);
    }

    /**
     * Can do incremental only if the last word is different.
     *
     * @param embedding
     * @return
     */
    private boolean canDoIncremental(Embedding embedding) {
        if (previousWords.size() != embedding.getNumWords()) {
            return false;
        }

        // Maximum we want 1 change (which we know by default that it exists in the last position).
        // so we check
        final int[] words = embedding.getWords();
        for (int i = previousWords.size() - 2; i >= 0; i--) {
            if (words[i] != previousWords.getUnchecked(i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int getNumberOfVertices() {
        return vertices.getSize();
    }

    @Override
    public boolean addEdge(int edgeId) {
        Edge<?> edge = mainGraph.getEdge(edgeId);

        int srcId = edge.getSourceId();
        int dstId = edge.getDestinationId();

        return addEdge(srcId, dstId);
    }

    private boolean addEdgeWithPositions(int edgeId) {
        Edge<?> edge = mainGraph.getEdge(edgeId);

        int srcId = edge.getSourceId();
        int dstId = edge.getDestinationId();

        Vertex<?> src = mainGraph.getVertex(srcId);
        Vertex<?> dst = mainGraph.getVertex(dstId);

        int srcPos = vertexPositions.get(srcId);
        int dstPos = vertexPositions.get(dstId);

        return addEdgeWithPositions(srcPos, src.getVertexLabel(), dstPos, dst.getVertexLabel());
    }

    public boolean addEdge(int srcId, int dstId) {
        Vertex<?> src = mainGraph.getVertex(srcId);
        Vertex<?> dst = mainGraph.getVertex(dstId);

        return addEdge(srcId, src.getVertexLabel(), dstId, dst.getVertexLabel());
    }

    public boolean addEdge(int srcId, int srcLabel, int dstId, int dstLabel) {
        int srcPos = addVertex(srcId);
        int dstPos = addVertex(dstId);

        return addEdgeWithPositions(srcPos, srcLabel, dstPos, dstLabel);
    }

    private boolean addEdgeWithPositions(int srcPos, int srcLabel, int dstPos, int dstLabel) {
        int minPos;
        int minLbl;
        int maxPos;
        int maxLbl;

        if (srcPos < dstPos) {
            minPos = srcPos;
            minLbl = srcLabel;
            maxPos = dstPos;
            maxLbl = dstLabel;
        } else {
            maxPos = srcPos;
            maxLbl = srcLabel;
            minPos = dstPos;
            minLbl = dstLabel;
        }

        PatternEdge patternEdge = createPatternEdge(minPos, minLbl, maxPos, maxLbl, true);

        return addEdge(patternEdge);
    }

    public boolean addEdge(PatternEdge edge) {
        edges.add(edge);

        setDirty();

        return  true;
    }

    public int addVertex(int vertexId) {
        int pos = vertexPositions.get(vertexId);

        if (pos == -1) {
            pos = vertices.size();
            vertices.add(vertexId);
            vertexPositions.put(vertexId, pos);
            setDirty();
        }

        return pos;
    }

    protected void setDirty() {
        dirtyCanonicalLabelling = true;
        dirtyVertexPositionEquivalences = true;
    }

    @Override
    public int getNumberOfEdges() {
        return edges.size();
    }

    @Override
    public IntArrayList getVertices() {
        return vertices;
    }

    @Override
    public PatternEdgeArrayList getEdges() {
        return edges;
    }

    @Override
    public VertexPositionEquivalences getVertexPositionEquivalences() {
        if (dirtyVertexPositionEquivalences) {
            synchronized (this) {
                if (dirtyVertexPositionEquivalences) {
                    if (vertexPositionEquivalences == null) {
                        vertexPositionEquivalences = new VertexPositionEquivalences();
                    }

                    vertexPositionEquivalences.setNumVertices(getNumberOfVertices());
                    vertexPositionEquivalences.clear();

                    fillVertexPositionEquivalences(vertexPositionEquivalences);

                    dirtyVertexPositionEquivalences = false;
                }
            }
        }

        return vertexPositionEquivalences;
    }

    protected abstract void fillVertexPositionEquivalences(VertexPositionEquivalences vertexPositionEquivalences);

    @Override
    public IntIntMap getCanonicalLabeling() {
        if (dirtyCanonicalLabelling) {
            synchronized (this) {
                if (dirtyCanonicalLabelling) {
                    if (canonicalLabelling == null) {
                        canonicalLabelling = HashIntIntMaps.newMutableMap(getNumberOfVertices());
                    }

                    canonicalLabelling.clear();

                    fillCanonicalLabelling(canonicalLabelling);

                    dirtyCanonicalLabelling = false;
                }
            }
        }

        return canonicalLabelling;
    }

    protected abstract void fillCanonicalLabelling(IntIntMap canonicalLabelling);

    @Override
    public boolean turnCanonical() {
        resetIncremental();

        IntIntMap canonicalLabelling = getCanonicalLabeling();

        IntIntCursor canonicalLabellingCursor = canonicalLabelling.cursor();

        boolean allEqual = true;

        while (canonicalLabellingCursor.moveNext()) {
            int oldPos = canonicalLabellingCursor.key();
            int newPos = canonicalLabellingCursor.value();

            if (oldPos != newPos) {
                allEqual = false;
            }
        }

        if (allEqual) {
            return false;
        }

        IntArrayList oldVertices = new IntArrayList(vertices);

        for (int i = 0; i < vertices.size(); ++i) {
            int newPos = canonicalLabelling.get(i);

            // If position didn't change, do nothing
            if (newPos == i) {
                continue;
            }

            int vertexId = oldVertices.get(i);
            vertices.set(newPos, vertexId);

            vertexPositions.put(vertexId, newPos);
        }

        for (int i = 0; i < edges.size(); ++i) {
            PatternEdge edge = edges.get(i);

            int srcPos = edge.getSrcId();
            int dstPos = edge.getDestId();

            int convertedSrcPos = canonicalLabelling.get(srcPos);
            int convertedDstPos = canonicalLabelling.get(dstPos);

            if (convertedSrcPos < convertedDstPos) {
                edge.setSrcId(convertedSrcPos);
                edge.setDestId(convertedDstPos);
            } else {
                // If we changed the position of source and destination due to
                // relabel, we also have to change the labels to match this
                // change.
                int tmp = edge.getSrcLabel();
                edge.setSrcId(convertedDstPos);
                edge.setSrcLabel(edge.getDestLabel());
                edge.setDestId(convertedSrcPos);
                edge.setDestLabel(tmp);
            }
        }

        return true;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        edges.write(dataOutput);
        vertices.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();

        edges.readFields(dataInput);
        vertices.readFields(dataInput);

        for (int i = 0; i < vertices.size(); ++i) {
            vertexPositions.put(vertices.get(i), i);
        }
    }

    protected PatternEdgeArrayList createPatternEdgeArrayList() {
        return new PatternEdgeArrayList();
    }

    protected PatternEdge createPatternEdge(int srcPos, int srcLabel, int dstPos, int dstLabel, boolean isForward) {
        PatternEdge patternEdge;

        if (patternEdgeListPool != null && !patternEdgeListPool.isEmpty()) {
            patternEdge = patternEdgeListPool.remove(patternEdgeListPool.size() - 1);
        } else {
            patternEdge = new PatternEdge();
        }

        patternEdge.setSrcId(srcPos);
        patternEdge.setDestId(dstPos);

        patternEdge.setSrcLabel(srcLabel);
        patternEdge.setDestLabel(dstLabel);

        patternEdge.isForward(isForward);

        return patternEdge;
    }

    protected void reclaimPatternEdge(PatternEdge patternEdge) {
        if (patternEdgeListPool == null) {
            patternEdgeListPool = new PatternEdgeArrayList();
        }

        patternEdgeListPool.add(patternEdge);
    }

    protected void reclaimPatternEdges(Collection<PatternEdge> patternEdges) {
        if (patternEdgeListPool == null) {
            patternEdgeListPool = new PatternEdgeArrayList();
        }

        patternEdgeListPool.ensureCapacity(patternEdgeListPool.size() + patternEdges.size());

        for (PatternEdge patternEdge : patternEdges) {
            reclaimPatternEdge(patternEdge);
        }
    }

    @Override
    public String toString() {
        return toOutputString();
    }

    @Override
    public String toOutputString() {
        if (getNumberOfEdges() > 0) {
            StringBuilder strBuilder = new StringBuilder();
            boolean first = true;

            for (PatternEdge edge : edges) {
                if (!first) {
                    strBuilder.append(',');
                }

                strBuilder.append(edge.getSrcId());
                strBuilder.append("(");
                strBuilder.append(edge.getSrcLabel());
                strBuilder.append(")");
                strBuilder.append('-');
                strBuilder.append(edge.getDestId());
                strBuilder.append("(");
                strBuilder.append(edge.getDestLabel());
                strBuilder.append(")");
                first = false;
            }

            return strBuilder.toString();
        } else {
            Vertex<?> vertex = mainGraph.getVertex(vertices.get(0));

            return "0(" + vertex.getVertexLabel() + ")";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicPattern that = (BasicPattern) o;

        return edges.equals(that.edges);

    }

    @Override
    public int hashCode() {
        return edges.hashCode();
    }

    protected int getVertexPositionAtId(int pos) {
        return vertexPositions.get(pos);
    }

    protected IntCollection getVertexPositions() {
        return vertexPositions.values();
    }
}
