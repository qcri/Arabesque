package io.arabesque.embedding;

import io.arabesque.graph.Edge;
import io.arabesque.graph.VertexNeighbourhood;
import io.arabesque.utils.DumpSetOrdered;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

public class EdgeInducedEmbedding extends BasicEmbedding {
    private HashIntSet[] incrementalEdgesAdded;
    protected HashIntSet extensionEdgeIds;

    private DumpSetOrdered vertices;
    private int[] numAdded;

    private IntCollectionAddConsumer mySetConsumer;

    public EdgeInducedEmbedding() {
        super();
        vertices = new DumpSetOrdered(INC_ARRAY_SIZE);
        numAdded = new int[INC_ARRAY_SIZE];
        extensionEdgeIds = HashIntSets.newMutableSet();
        incrementalEdgesAdded = new HashIntSet[INC_ARRAY_SIZE];
        mySetConsumer = new IntCollectionAddConsumer();
    }

    public IntCollection getExtensibleWordIds() {
        return getExtensibleEdgeIds();
    }

    @Override
    public String toOutputString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < numWords; ++i) {
            Edge edge = g.getEdge(words[i]);
            sb.append(edge.getSourceId());
            sb.append("-");
            sb.append(edge.getDestinationId());
            sb.append(" ");
        }

        return sb.toString();
    }

    public IntCollection getExtensibleEdgeIds() {
        ++total;
        if (vertices.getNumKeys() > 2) {
            int common = canDoIncremental(vertices.getKeys(), vertices.getNumKeys());
            if (common > 1) {
                ++incremental;
                return getExtensibleIdsIncrementalValue(vertices.getKeys(),
                        vertices.getNumKeys(),
                        common);
            }

            if (numWords == 0) {
                return getForZeroWords();
            }

            return getExtensibleIdsScratchValue(vertices.getKeys(),
                    vertices.getNumKeys());
        }

        if (numWords == 0) {
            return getForZeroWords();
        }
        return getExtensibleIdsScratchValue(vertices.getKeys(),
                vertices.getNumKeys());

    }


    /**
     * Instead of starting empty, we are starting from the set that correspond to the
     * position that we have the last common with the previous.
     *
     * @param common
     * @return
     */
    IntCollection getExtensibleIdsIncrementalValue(final int[] vertA,
            final int numVertices,
            final int common) {
        // or the following
        //extensionEdgeIds = HashIntSets.newMutableSet(incrementalEdgesAdded[common - 1]);

        extensionEdgeIds.clear();
        extensionEdgeIds.addAll(incrementalEdgesAdded[common - 1]);

        previousWordsPos = numVertices;

        mySetConsumer.setCollection(extensionEdgeIds);

        for (int i = common; i < numVertices; i++) {
            final int vertexId = vertA[i];
            final VertexNeighbourhood neighbourhood = g.getVertexNeighbourhood(vertexId);
            neighbourhood.getNeighbourEdges().forEach(mySetConsumer);

            // We ignore the last one since it always changes!!!
            if (i < numVertices - 1) {
                if (incrementalEdgesAdded[i] == null) {
                    incrementalEdgesAdded[i] = HashIntSets.newMutableSet(extensionEdgeIds);
                } else {
                    incrementalEdgesAdded[i].clear();
                    incrementalEdgesAdded[i].addAll(extensionEdgeIds);
                }
            }
            previousVertices[i] = vertexId;
        }

        // Clean the ones that exist already!!!.
        for (int i = 0; i < numWords; ++i) {
            int edgeId = words[i];
            extensionEdgeIds.removeInt(edgeId);
        }

        return extensionEdgeIds;
    }

    /**
     * @param vertA
     * @param numC
     * @return
     */
    IntCollection getExtensibleIdsScratchValue(final int[] vertA,
            final int numC) {
        extensionEdgeIds.clear();

        previousWordsPos = numC;
        mySetConsumer.setCollection(extensionEdgeIds);
        for (int i = 0; i < numC; i++) {
            final int vertexId = vertA[i];

            final VertexNeighbourhood neighbourhood = g.getVertexNeighbourhood(vertexId);
            neighbourhood.getNeighbourEdges().forEach(mySetConsumer);

            if (numC > 2) {
                //Prepare for incremental.
                if (incrementalEdgesAdded[i] == null) {
                    incrementalEdgesAdded[i] =
                            HashIntSets.newMutableSet(extensionEdgeIds);
                } else {
                    incrementalEdgesAdded[i].clear();
                    incrementalEdgesAdded[i].addAll(extensionEdgeIds);
                }

                previousVertices[i] = vertexId;
            }
        }

        for (int i = 0; i < numWords; ++i) {
            int edgeId = words[i];
            extensionEdgeIds.removeInt(edgeId);
        }
        return extensionEdgeIds;
    }


    /**
     * @return
     */
    private HashIntSet getForZeroWords() {
        extensionEdgeIds.clear();
        int numEdges = g.getNumberEdges();
        for (int edgeId = 0; edgeId < numEdges; ++edgeId) {
            extensionEdgeIds.add(edgeId);
        }
        return extensionEdgeIds;
    }

    public void printStats() {
        System.out.println("Had Total:" + total + "   Incremental:" + incremental);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Embedding{" +
                "words=[");
        for (int i = 0; i < numWords; ++i) {
            sb.append(g.getEdge(words[i]));
            sb.append(", ");
        }
        sb.append(
                "], numWords=" + numWords +
                        "} " + super.toString());
        return sb.toString();
    }

    public boolean isConnected() {
        for (int i = 0; i < numWords; ++i) {
            boolean foundConnection = false;

            final Edge edge = g.getEdge(words[i]);
            int src1 = edge.getSourceId();
            int dest1 = edge.getDestinationId();


            for (int j = 0; j < numWords; ++j) {
                if (i == j) {
                    continue;
                }

                if (g.isNeighborEdge(src1, dest1, words[j])) {
                    foundConnection = true;
                    break;
                }
            }

            if (!foundConnection) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isCanonicalEmbeddingWithWord(int wordId) {

        int i = 0;
        if (this.numWords == 0) return true;
        if (wordId < words[0]) return false;

        //find the first edge neighbor
        while (i < numWords) {
            if (g.areEdgesNeighbors(wordId, words[i])) {
                break;
            }
            i++;
        }

        i++;
        //now canonicality testing
        while (i < numWords) {
            if (words[i] >= wordId) {
                return false;
            }
            i++;
        }
        return true;
    }

    /**
     * Add word and update the number of vertices in this embedding.
     *
     * @param word
     */
    @Override
    public void addWord(int word) {
        super.addWord(word);
        updateVertices(word);
    }

    private void updateVertices(int word) {
        final Edge edge = g.getEdge(word);

        int added = 0;
        if (vertices.add(edge.getSourceId())) {
            ++added;
        }

        if (vertices.add(edge.getDestinationId())) {
            ++added;
        }

        if (numAdded.length < numWords) {
            numAdded = Arrays.copyOf(numAdded, numAdded.length + INC_ARRAY_SIZE);
        }
        numAdded[numWords - 1] = added;

    }

    @Override
    public void removeLastWord() {
        vertices.remove(numAdded[numWords - 1]);
        super.removeLastWord();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();

        super.readFields(in);

        for (int i = 0; i < numWords; ++i) {
            updateVertices(words[i]);
        }
    }

    @Override
    public void reset() {
        super.reset();
        vertices.reset();
    }

    @Override
    public int[] getVertices() {
        return vertices.getKeys();
    }

    @Override
    public int getNumVertices() {
        return vertices.getNumKeys();
    }

    @Override
    public int[] getEdges() {
        return getWords();
    }

    @Override
    public int getNumEdges() {
        return getNumWords();
    }

    /**
     * How many it added? Should compute...
     *
     * @return
     */
    @Override
    public int getNumVerticesAddedWithExpansion() {
        return numAdded[numWords - 1];
    }

    /**
     * By default we grow by edges, so the last one is a single edge.
     *
     * @return
     */
    @Override
    public int getNumEdgesAddedWithExpansion() {
        return 1;
    }


}
