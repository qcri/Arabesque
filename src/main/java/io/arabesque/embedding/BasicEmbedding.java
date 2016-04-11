package io.arabesque.embedding;

import io.arabesque.conf.Configuration;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import net.openhft.koloboke.function.IntConsumer;
import net.openhft.koloboke.function.IntPredicate;

import java.io.DataOutput;
import java.io.ObjectOutput;
import java.io.IOException;
import java.util.Objects;

public abstract class BasicEmbedding implements Embedding {
    // Basic structure {{
    protected IntArrayList vertices;
    protected IntArrayList edges;

    // Extension helper structures {{
    protected HashIntSet extensionWordIds;
    protected boolean dirtyExtensionWordIds;
    protected ObjArrayList<IntArrayList> extensionWordIdsPerPos;
    protected IntArrayList previousExtensionCalculationVertices;

    private IntConsumer extensionWordIdsAdder = new IntConsumer() {
        @Override
        public void accept(int i) {
            extensionWordIds.add(i);
        }
    };

    private IntPredicate existsInExtensionWordIds = new IntPredicate() {
        @Override
        public boolean test(int i) {
            return extensionWordIds.contains(i);
        }
    };

    protected IntCollectionAddConsumer intAddConsumer = new IntCollectionAddConsumer();
    // }}

    // Pattern {{
    /**
     * Pattern associated with this embedding.
     *
     * Whether the current value actually represents the current embedding
     * depends on the value of the {@link #dirtyPattern} variable.
     */
    private Pattern pattern;
    /**
     * Whether the variable referred to in {@link #pattern} is up to date
     * with the structure of the embedding.
     */
    private boolean dirtyPattern;
    // }}

    // Incremental Stuff {{
    // }}

    // Helpers {{
    protected MainGraph mainGraph;
    // }}

    public BasicEmbedding() {
        init();
    }

    protected void init() {
        vertices = new IntArrayList();
        edges = new IntArrayList();

        mainGraph = Configuration.get().getMainGraph();

        extensionWordIds = HashIntSets.newMutableSet();
        previousExtensionCalculationVertices = new IntArrayList();

        extensionWordIdsPerPos = new ObjArrayList<>();

        reset();
    }

    public void reset() {
        vertices.clear();
        edges.clear();
        IntArrayListPool.instance().reclaimObjects(extensionWordIdsPerPos);
        extensionWordIdsPerPos.clear();
        previousExtensionCalculationVertices.clear();
        setDirty();
    }

    protected void setDirty() {
        dirtyPattern = true;
        dirtyExtensionWordIds = true;
    }

    @Override
    public IntArrayList getVertices() {
        return vertices;
    }

    @Override
    public int getNumVertices() {
        return vertices.size();
    }

    @Override
    public IntArrayList getEdges() {
        return edges;
    }

    @Override
    public int getNumEdges() {
        return edges.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        getWords().write(out);
    }
    
    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write (objOutput);
    }

    @Override
    public IntCollection getExtensibleWordIds() {
        // If we have to recompute the extensionVertexIds set
        if (dirtyExtensionWordIds) {
            updateExtensibleWordIdsSimple();
        }

        return extensionWordIds;
    }

    protected void updateExtensibleWordIdsSimple() {
        IntArrayList vertices = getVertices();
        int numVertices = getNumVertices();

        extensionWordIds.clear();

        for (int i = 0; i < numVertices; ++i) {
            IntCollection neighbourhood = getValidNeighboursForExpansion(vertices.getUnchecked(i));

            if (neighbourhood != null) {
                neighbourhood.forEach(extensionWordIdsAdder);
            }
        }

        IntArrayList words = getWords();
        int numWords = getNumWords();

        // Clean the words that are already in the embedding
        for (int i = 0; i < numWords; ++i) {
            int wId = words.getUnchecked(i);
            extensionWordIds.removeInt(wId);
        }
    }

    @Override
    public boolean isCanonicalEmbeddingWithWord(int wordId) {
        IntArrayList words = getWords();
        int numWords = words.size();

        if (numWords == 0) return true;
        if (wordId < words.getUnchecked(0)) return false;

        int i;

        // find the first neighbor
        for (i = 0; i < numWords; ++i) {
            if (areWordsNeighbours(wordId, words.getUnchecked(i))) {
                break;
            }
        }

        // if we didn't find any neighbour
        if (i == numWords) {
            // not canonical because it's disconnected
            return false;
        }

        // If we found the first neighbour, all following words should have lower
        // ids than the one we are trying to add
        i++;
        for (; i < numWords; ++i) {
            // If one of those ids is higher or equal, not canonical
            if (words.getUnchecked(i) >= wordId) {
                return false;
            }
        }

        return true;
    }

    protected abstract boolean areWordsNeighbours(int wordId1, int wordId2);

    protected abstract IntCollection getValidNeighboursForExpansion(int vId);

    @Override
    public void addWord(int word) {
        setDirty();
    }

    @Override
    public void removeLastWord() {
        setDirty();
    }

    @Override
    public String toString() {
        return "Embedding{" +
                "vertices=" + vertices + ", " +
                "edges=" + edges +
                "} " + super.toString();
    }

    @Override
    public Pattern getPattern() {
        if (dirtyPattern) {
            if (pattern == null) {
                pattern = Configuration.get().createPattern();
            }

            pattern.setEmbedding(this);
            dirtyPattern = false;
        }

        return pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BasicEmbedding that = (BasicEmbedding) o;
        return Objects.equals(vertices, that.vertices) &&
                Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertices, edges);
    }
}
