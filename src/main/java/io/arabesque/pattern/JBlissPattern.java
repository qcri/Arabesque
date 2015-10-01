package io.arabesque.pattern;

import fi.tkk.ics.jbliss.Graph;
import fi.tkk.ics.jbliss.Reporter;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.utils.DumpHash;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMapFactory;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by afonseca on 3/15/2015.
 */
public class JBlissPattern extends Pattern {
    private static final Logger LOG = Logger.getLogger(JBlissPattern.class);

    private static HashIntIntMapFactory intIntMapFactory
            = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1);
    private final Graph<Integer> jblissGraph;

    private int numEdges;
    private PatternEdge[] edges;
    private int[] previousWords;
    private int previousNumWords = -1;

//    private HashIntIntMap vertexPositions;

    // Map<Real vertex ID, Index in vertices[]>
    private DumpHash vertexPositions;

    private int numVertices;
    private int[] vertices;
    private HashIntIntMap canonicalLabeling;
    private final MainGraph<Vertex<?>, ?, ?, Edge<?>> g;

    private int lastVertexWordPos = -1;
    private int previousAddedEdges = 0;
    private int previousAddedVertices = 0;
    private int incremental = 0;
    private int total = 0;
    private boolean printed = false;
    private boolean dirtyAutoVertexSet;
    private HashIntSet[] autoVertexSet;

    public JBlissPattern() {
        edges = null;
        vertices = null;
        vertexPositions = null;
        g = MainGraph.get();
        reset();
        jblissGraph = new Graph<>(this);
    }

    public JBlissPattern(JBlissPattern other) {
        reset();
        g = MainGraph.get();

        numEdges = other.numEdges;

        edges = new PatternEdge[numEdges];

        for (int i = 0; i < numEdges; ++i) {
            edges[i] = new PatternEdge(other.edges[i]);
        }

        numVertices = other.numVertices;

        vertices = Arrays.copyOf(other.vertices, numVertices);
        vertexPositions = null;

        canonicalLabeling = null;

        if (other.canonicalLabeling != null) {
            canonicalLabeling = HashIntIntMaps.newMutableMap(other.canonicalLabeling);
        }

        this.total = other.total;
        this.incremental = other.incremental;

        jblissGraph = new Graph<>(this);
    }

    @Override
    public Pattern copy() {
        return new JBlissPattern(this);
    }

    private void resetIncremental() {
        numEdges -= previousAddedEdges;
        vertexPositions.setIndex(lastVertexWordPos);
        numVertices = lastVertexWordPos;
        dirtyAutoVertexSet = true;
    }


    @Override
    public void reset() {
        numEdges = 0;
        numVertices = 0;

        if (vertexPositions != null) {
            vertexPositions.clear();
        } else {
            vertexPositions = new DumpHash(15);
//            vertexPositions = intIntMapFactory.newMutableMap();
        }

        if (canonicalLabeling != null) {
            canonicalLabeling.clear();
        }

        dirtyAutoVertexSet = true;
    }

    /**
     * Can do incremental only if the last word is different.
     *
     * @param embedding
     * @return
     */
    private boolean canDoIncremental(Embedding embedding) {
        // Test without incremental
        /*if (true != false) {
            return false;
        }*/

        if (previousNumWords != embedding.getNumWords() || previousNumWords <= 1) {
            return false;
        }

        // Maximum we want 1 change (which we know by default that it exists in the last position).
        // so we check
        final int[] words = embedding.getWords();
        for (int i = previousNumWords - 2; i >= 0; i--) {
            if (words[i] != previousWords[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param embedding
     */
    public void setEmbedding(Embedding embedding) {
        ++total;
        if (canDoIncremental(embedding)) {
            ++incremental;
            setEmbeddingIncremental(embedding);
        } else {
            setEmbeddingFromScratch(embedding);
        }
    }

    /**
     * Only the last word has changed, so skipped processing for the previous words.
     *
     * @param embedding
     */
    private void setEmbeddingIncremental(Embedding embedding) {
//        if (embedding.numWords == 1) {
//            LOG.info("Incremental:" + Arrays.toString(embedding.words) + "\t" + vertexPositions + "\t" + Arrays.toString(vertices));
//        }
        resetIncremental();
        previousAddedVertices = embedding.getNumVerticesAddedWithExpansion();
        previousAddedEdges = embedding.getNumEdgesAddedWithExpansion();
        int numEdgesInEmbedding = embedding.getNumEdges();
        int numVerticesInEmbedding = embedding.getNumVertices();
//        LOG.info("\tlastVertexWordPos:"+lastVertexWordPos+"\tPAddedVertices:"+previousAddedVertices+"   pAddedEdges:"+previousAddedEdges
//                 +" nV:"+numVerticesInEmbedding+" nE:"+numEdgesInEmbedding + Arrays.toString(embedding.getVertices()));

//        ensureCanStoreNewVertices(previousAddedVertices);
        ensureCanStoreNewEdges(previousAddedEdges);

        int[] embeddingVertices = embedding.getVertices();

        for (int i = (numVerticesInEmbedding - previousAddedVertices); i < numVerticesInEmbedding; ++i) {
            addVertex(embeddingVertices[i]);
        }

        int[] embeddingEdges = embedding.getEdges();

        for (int i = (numEdgesInEmbedding - previousAddedEdges); i < numEdgesInEmbedding; ++i) {
            addEdgeInc(embeddingEdges[i]);
        }

        updateUsedEmbeddingIncremental(embedding);
//        if (embedding.numWords == 1) {
//            LOG.info("After \t Incremental:" + Arrays.toString(embedding.words) + "\t" + vertexPositions + "\t" + Arrays.toString(vertices));
//        }
    }

    /**
     * By default only the last word changed.
     *
     * @param embedding
     */
    private void updateUsedEmbeddingIncremental(Embedding embedding) {
        previousWords[previousNumWords - 1] = embedding.getWords()[previousNumWords - 1];
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

        lastVertexWordPos = numVertices - embedding.getNumVerticesAddedWithExpansion();
        //Next add the last...

        int[] embeddingEdges = embedding.getEdges();

        for (int i = 0; i < numEdgesInEmbedding; ++i) {
            addEdge(embeddingEdges[i]);
        }

        previousAddedEdges = embedding.getNumEdgesAddedWithExpansion();

        updateUsedEmbeddingFromScratch(embedding);
//        if (embedding.numWords == 1) {
//            LOG.info("Scratch:" + Arrays.toString(embedding.words) + "\t" + vertexPositions + "\t" + Arrays.toString(vertices));
//        }
    }

    /**
     * @param embedding
     */
    private void updateUsedEmbeddingFromScratch(Embedding embedding) {
        previousNumWords = embedding.getNumWords();

        if (previousWords == null || previousWords.length < previousNumWords) {
            previousWords = Arrays.copyOf(embedding.getWords(), embedding.getWords().length);
        } else {
            int[] words = embedding.getWords();
            for (int i = 0; i < previousNumWords; i++) {
                previousWords[i] = words[i];
            }
        }
    }

    @Override
    public int getNumberOfVertices() {
        return numVertices;
    }

    /**
     * @param numberOfNewEdges
     */
    protected void ensureCanStoreNewEdges(int numberOfNewEdges) {
        ensureCanStoreNewEdges(numberOfNewEdges, false);
    }

    /**
     * @param numberOfNewEdges
     * @param exact
     */
    protected void ensureCanStoreNewEdges(int numberOfNewEdges, boolean exact) {
        if (edges == null) {
            edges = new PatternEdge[numberOfNewEdges];
            return;
        }

        if (edges.length - numEdges < numberOfNewEdges) {
            int newSize;

            if (exact) {
                newSize = numEdges + numberOfNewEdges;
            } else {
                newSize = (edges.length + numberOfNewEdges) * 2;
            }

            edges = Arrays.copyOf(edges, newSize);
        }
    }

//    protected void ensureCanStoreNewEdge() {
//        ensureCanStoreNewEdges(1);
//    }

    protected void ensureCanStoreNewVertices(int numberOfNewVertices) {
        ensureCanStoreNewVertices(numberOfNewVertices, false);
    }

    protected void ensureCanStoreNewVertices(int numberOfNewVertices, boolean exact) {
        if (vertices == null) {
            vertices = new int[numberOfNewVertices];
            return;
        }

        if (vertices.length - numVertices < numberOfNewVertices) {
            int newSize;

            if (exact) {
                newSize = numVertices + numberOfNewVertices;
            } else {
                newSize = (vertices.length + numberOfNewVertices) * 2;
            }

            vertices = Arrays.copyOf(vertices, newSize);
        }
    }

    @Override
    public boolean addEdge(int edgeId) {
        Edge<?> edge = g.getEdge(edgeId);

        int srcId = edge.getSourceId();
        int dstId = edge.getDestinationId();

        Vertex<?> src = g.getVertex(srcId);
        Vertex<?> dst = g.getVertex(dstId);

        return addEdge(src, dst);
    }

    public PatternEdge[] getEdges() {
        return edges;
    }

    public DumpHash getVertexPositions() {
        return vertexPositions;
    }

    @Override
    public boolean addEdge(PatternEdge patternEdge) {
        return addEdge(patternEdge.getSrcId(), patternEdge.getSrcLabel(),
                patternEdge.getDestId(), patternEdge.getDestLabel());
    }

    public boolean addEdge(Vertex<?> src, Vertex<?> dst) {
        return addEdge(src.getVertexId(), src.getVertexLabel(), dst.getVertexId(), dst.getVertexLabel());
    }

    public boolean addEdgeTest(Vertex<?> src, Vertex<?> dst) {
        ensureCanStoreNewVertices(2);
        ensureCanStoreNewEdges(1);
        return addEdge(src.getVertexId(), src.getVertexLabel(), dst.getVertexId(), dst.getVertexLabel());
    }

    public int addVertex(int vertexId) {
        //ensureVertexPositionsAreReady();

        int pos = vertexPositions.get(vertexId);

        if (pos == -1) {
            ensureCanStoreNewVertices(1);
            pos = numVertices;
            numVertices++;
            vertices[pos] = vertexId;
            vertexPositions.put(vertexId, pos);
        }

        dirtyAutoVertexSet = true;

        return pos;
    }

    /**
     * We know that all vertices exist, so just query the vertices.
     * We pass a parameter whether these additions are due to the last word.
     *
     * @param edgeId
     */
    private boolean addEdgeInc(int edgeId) {
        Edge<?> edge = g.getEdge(edgeId);

        int srcEId = edge.getSourceId();
        int dstEId = edge.getDestinationId();

        Vertex<?> src = g.getVertex(srcEId);
        Vertex<?> dst = g.getVertex(dstEId);
        int srcId = src.getVertexId();
        int srcLabel = src.getVertexLabel();
        int dstId = dst.getVertexId();
        int dstLabel = dst.getVertexLabel();

        int thisEdgeIndex = numEdges;

        PatternEdge patternEdge = edges[thisEdgeIndex];

        if (patternEdge == null) {
            patternEdge = edges[thisEdgeIndex] = new PatternEdge();
        }

        int srcPos = vertexPositions.get(srcId);
        int dstPos = vertexPositions.get(dstId);

        if (srcPos < dstPos) {
            patternEdge.setSrcId(srcPos);
            patternEdge.setDestId(dstPos);

            patternEdge.setSrcLabel(srcLabel);
            patternEdge.setDestLabel(dstLabel);
        } else {
            patternEdge.setSrcId(dstPos);
            patternEdge.setDestId(srcPos);

            patternEdge.setSrcLabel(dstLabel);
            patternEdge.setDestLabel(srcLabel);
        }

        numEdges++;

        dirtyAutoVertexSet = true;

        return true;
    }

    public boolean addEdge(int srcId, int srcLabel, int dstId, int dstLabel) {
//    	ensureCanStoreNewEdge();

        int thisEdgeIndex = numEdges;

        PatternEdge patternEdge = edges[thisEdgeIndex];

        if (patternEdge == null) {
            patternEdge = edges[thisEdgeIndex] = new PatternEdge();
        }

        int srcPos = addVertex(srcId);
        int dstPos = addVertex(dstId);

        if (srcPos < dstPos) {
            patternEdge.setSrcId(srcPos);
            patternEdge.setDestId(dstPos);

            patternEdge.setSrcLabel(srcLabel);
            patternEdge.setDestLabel(dstLabel);
        } else {
            patternEdge.setSrcId(dstPos);
            patternEdge.setDestId(srcPos);

            patternEdge.setSrcLabel(dstLabel);
            patternEdge.setDestLabel(srcLabel);
        }

        numEdges++;

        dirtyAutoVertexSet = true;

        return true;
    }

    @Override
    public int getNumberOfEdges() {
        return numEdges;
    }

    @Override
    public void generateMinPatternCode() {
        turnCanonical();
    }

    @Override
    public int[] getVertices() {
        return vertices;
    }

    @Override
    public PatternEdge[] getPatternEdges() {
        return edges;
    }

//    public HashIntIntMap getVertexPositions() {
//        return vertexPositions;
//    }

    protected class AutoVertexSetReporter implements Reporter {
        private HashIntSet[] autoVertexSet;
        private HashIntSet[] equivalences;
        private HashIntIntMap inverseCanonicalLabeling;

        public AutoVertexSetReporter(HashIntSet[] autoVertexSet, HashIntIntMap canonicalLabeling) {
            this.autoVertexSet = autoVertexSet;
            equivalences = new HashIntSet[autoVertexSet.length];
            this.inverseCanonicalLabeling = HashIntIntMaps.newMutableMap(canonicalLabeling.size());

            IntIntCursor canonicalLabelingCursor = canonicalLabeling.cursor();

            while (canonicalLabelingCursor.moveNext()) {
                inverseCanonicalLabeling.put(canonicalLabelingCursor.value(), canonicalLabelingCursor.key());
            }
        }

        @Override
        public void report(HashIntIntMap generator, Object user_param) {
            IntIntCursor generatorCursor = generator.cursor();

            while (generatorCursor.moveNext()) {
                int oldPos = generatorCursor.key();
                int newPos = generatorCursor.value();

                //autoVertexSet[oldPos].add(inverseCanonicalLabeling.get(newPos));
                autoVertexSet[inverseCanonicalLabeling.get(oldPos)].add(inverseCanonicalLabeling.get(newPos));

                if (oldPos != newPos) {
                    HashIntSet currentPosEquivalences = equivalences[oldPos];

                    if (currentPosEquivalences == null) {
                        currentPosEquivalences = HashIntSets.newMutableSet();
                        equivalences[oldPos] = currentPosEquivalences;
                    }

                    currentPosEquivalences.add(newPos);
                }
            }
        }

        public void applyEquivalences() {
            int numVertices = equivalences.length;

            for (int i = 0; i < numVertices; ++i) {
                HashIntSet currentVertexEquivalences = equivalences[i];

                if (currentVertexEquivalences != null) {
                    IntCursor cursor = currentVertexEquivalences.cursor();

                    while (cursor.moveNext()) {
                        int equivalentPosition = cursor.elem();

                        autoVertexSet[inverseCanonicalLabeling.get(i)].addAll(autoVertexSet[inverseCanonicalLabeling.get(equivalentPosition)]);
                    }
                }
            }
        }
    }

    protected class AutoVertexSetTestReporter implements Reporter {
        private HashIntSet[] autoVertexSet;
        private HashIntSet[] equivalences;

        public AutoVertexSetTestReporter(HashIntSet[] autoVertexSet) {
            this.autoVertexSet = autoVertexSet;
            equivalences = new HashIntSet[autoVertexSet.length];
        }

        @Override
        public void report(HashIntIntMap generator, Object user_param) {
            IntIntCursor generatorCursor = generator.cursor();

            while (generatorCursor.moveNext()) {
                int oldPos = generatorCursor.key();
                int newPos = generatorCursor.value();

                autoVertexSet[oldPos].add(newPos);

                if (oldPos != newPos) {
                    HashIntSet currentPosEquivalences = equivalences[oldPos];

                    if (currentPosEquivalences == null) {
                        currentPosEquivalences = HashIntSets.newMutableSet();
                        equivalences[oldPos] = currentPosEquivalences;
                    }

                    currentPosEquivalences.add(newPos);
                }
            }
        }

        public void applyEquivalences() {
            int numVertices = equivalences.length;

            for (int i = 0; i < numVertices; ++i) {
                HashIntSet currentVertexEquivalences = equivalences[i];

                if (currentVertexEquivalences != null) {
                    IntCursor cursor = currentVertexEquivalences.cursor();

                    while (cursor.moveNext()) {
                        int equivalentPosition = cursor.elem();

                        autoVertexSet[i].addAll(autoVertexSet[equivalentPosition]);
                    }
                }
            }
        }
    }

    @Override
    public HashIntSet[] getAutoVertexSet() {
        if (true != false) {
            return getAutoVertexSetTest();
        }
        if (dirtyAutoVertexSet) {
            synchronized (this) {
                if (dirtyAutoVertexSet) {
                    if (autoVertexSet == null || autoVertexSet.length < numVertices) {
                        autoVertexSet = new HashIntSet[numVertices];

                        for (int i = 0; i < numVertices; ++i) {
                            autoVertexSet[i] = HashIntSets.newMutableSet();
                        }
                    } else {
                        for (int i = 0; i < numVertices; ++i) {
                            autoVertexSet[i].clear();
                        }
                    }

                    for (int i = 0; i < numVertices; ++i) {
                        autoVertexSet[i].add(i);
                    }

                    AutoVertexSetReporter reporter = new AutoVertexSetReporter(autoVertexSet, canonicalLabeling);

                    jblissGraph.findAutomorphisms(reporter, null);

                    reporter.applyEquivalences();

                    dirtyAutoVertexSet = false;
                }
            }
        }

        return autoVertexSet;
    }

    public HashIntSet[] getAutoVertexSetTest() {
        if (dirtyAutoVertexSet) {
            synchronized (this) {
                if (dirtyAutoVertexSet) {
                    if (autoVertexSet == null || autoVertexSet.length < numVertices) {
                        autoVertexSet = new HashIntSet[numVertices];

                        for (int i = 0; i < numVertices; ++i) {
                            autoVertexSet[i] = HashIntSets.newMutableSet();
                        }
                    } else {
                        for (int i = 0; i < numVertices; ++i) {
                            autoVertexSet[i].clear();
                        }
                    }

                    for (int i = 0; i < numVertices; ++i) {
                        autoVertexSet[i].add(i);
                    }

                    AutoVertexSetTestReporter reporter = new AutoVertexSetTestReporter(autoVertexSet);

                    jblissGraph.findAutomorphisms(reporter, null);

                    reporter.applyEquivalences();

                    dirtyAutoVertexSet = false;
                }
            }
        }

        return autoVertexSet;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        sb.append("Vertices: ");
        for (int i = 0; i < numVertices; ++i) {
            if (!first) {
                sb.append(',');
            }
            sb.append(vertices[i]);
            first = false;
        }

        sb.append("   Edges: ");
        for (int i = 0; i < numEdges; ++i) {
            if (!first) {
                sb.append(',');
            }
            PatternEdge edge = edges[i];
            sb.append(edge.getSrcId());
            sb.append("(");
            sb.append(edge.getSrcLabel());
            sb.append(")");
            sb.append('-');
            sb.append(edge.getDestId());
            sb.append("(");
            sb.append(edge.getDestLabel());
            sb.append(")");
            first = false;
        }

        return sb.toString();
    }

    public void turnCanonical() {
//        if (!printed){
//            System.out.println("Total:"+total+ " Incremental:"+incremental);
//            printed = true;
//        }

        canonicalLabeling = jblissGraph.getCanonicalLabeling();

        int[] oldVertices = Arrays.copyOf(vertices, numVertices);

        for (int i = 0; i < numVertices; ++i) {
            int newPos = canonicalLabeling.get(i);

            // If position didn't change, do nothing
            if (newPos == i) {
                continue;
            }

            int vertexId = oldVertices[i];
            vertices[newPos] = vertexId;

            if (vertexPositions != null) {
                vertexPositions.put(vertexId, newPos);
            }
        }

        for (int i = 0; i < numEdges; ++i) {
            PatternEdge edge = edges[i];

            int srcPos = edge.getSrcId();
            int dstPos = edge.getDestId();

            int convertedSrcPos = canonicalLabeling.get(srcPos);
            int convertedDstPos = canonicalLabeling.get(dstPos);

            if (convertedSrcPos < convertedDstPos) {
                edge.setSrcId(convertedSrcPos);
                edge.setDestId(convertedDstPos);
            } else {
                edge.setSrcId(convertedDstPos);
                edge.setDestId(convertedSrcPos);
            }
        }

        Arrays.sort(edges, 0, numEdges);
        dirtyAutoVertexSet = true;
    }

    private static <T extends Comparable> void insertionSort(T[] arr, int max) {
        int j;
        T key;
        int i;

        for (i = 1; i < max; ++i) {
            key = arr[i];

            j = i;

            while (j > 0 && arr[j - 1].compareTo(key) == 1) {
                arr[j] = arr[j - 1];
                --j;
            }

            arr[j] = key;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JBlissPattern that = (JBlissPattern) o;

        if (numEdges != that.numEdges) return false;
        for (int i = 0; i < numEdges; ++i) {
            if (!edges[i].equals(that.edges[i])) {
                return false;
            }
        }
        //if (!vertexPositions.equals(that.vertexPositions)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = numEdges;

        if (edges != null) {
            for (int i = 0; i < numEdges; ++i) {
                PatternEdge edge = edges[i];
                result = 31 * result + (edge == null ? 0 : edge.hashCode());
            }
        } else {
            result = 31 * result;
        }

        //result = 31 * result + vertexPositions.hashCode();
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(numEdges);

        for (int i = 0; i < numEdges; ++i) {
            edges[i].write(dataOutput);
        }

        int numVertices = getNumberOfVertices();
        dataOutput.writeInt(numVertices);

        for (int i = 0; i < numVertices; ++i) {
            dataOutput.writeInt(vertices[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();

        numEdges = dataInput.readInt();
        ensureCanStoreNewEdges(numEdges, true);

        for (int i = 0; i < numEdges; ++i) {
            PatternEdge patternEdge = new PatternEdge();
            patternEdge.readFields(dataInput);

            edges[i] = patternEdge;
        }

        numVertices = dataInput.readInt();
        ensureCanStoreNewVertices(numVertices, true);

        for (int i = 0; i < numVertices; ++i) {
            int vertexId = dataInput.readInt();
            vertices[i] = vertexId;
        }
    }

    @Override
    public HashIntIntMap getCanonicalLabeling() {
        return canonicalLabeling;
    }
}
