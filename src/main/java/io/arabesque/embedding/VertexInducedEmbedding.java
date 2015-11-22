package io.arabesque.embedding;

import io.arabesque.graph.Vertex;
import io.arabesque.utils.ElementCounterConsumer;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import net.openhft.koloboke.function.IntConsumer;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

public class VertexInducedEmbedding extends BasicEmbedding {
    HashIntSet extensionVertexIds;
    HashIntIntMap extensionVertexIdMap;
    private HashIntIntMap[] incrementalVertexCount;

    int[] edges;
    int[] numAdded;

    int edgesSize;
    private ElementCounterConsumer elementCounterConsumer;
    private UpdateEdgesConsumer updateEdgesConsumer;

    public VertexInducedEmbedding() {
        super();
        edges = new int[INC_ARRAY_SIZE];
        extensionVertexIds = HashIntSets.newMutableSet();
        extensionVertexIdMap = HashIntIntMaps.newMutableMap();

        edges = new int[INC_ARRAY_SIZE];
        numAdded = new int[INC_ARRAY_SIZE];
        incrementalVertexCount = new HashIntIntMap[INC_ARRAY_SIZE];
        elementCounterConsumer = new ElementCounterConsumer();
        updateEdgesConsumer = new UpdateEdgesConsumer();
    }

    @Override
    public void reset() {
        super.reset();
        edgesSize = 0;
    }

    @Override
    public int[] getVertices() {
        return getWords();
    }

    @Override
    public int getNumVertices() {
        return getNumWords();
    }

    @Override
    public String toOutputString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < numWords; ++i) {
            Vertex vertex = g.getVertex(words[i]);
            sb.append(vertex.getVertexId());
            sb.append(" ");
        }

        return sb.toString();
    }


    /**
     * By default we grow by vertices, so the word corresponds to one vertex.
     *
     * @return
     */
    @Override
    public int getNumVerticesAddedWithExpansion() {
        if (numWords == 0) {
            return 0;
        }

        return 1;
    }

    @Override
    public int getNumEdgesAddedWithExpansion() {
        if (numWords == 0) {
            return 0;
        }

        return numAdded[numWords - 1];
    }

    @Override
    public boolean isCanonicalEmbeddingWithWord(int wordId) {
        if (this.numWords == 0) return true;
        if (wordId < words[0]) return false;

        int i = 0;

        //find the first edge neighbor
        while (i < numWords) {
            if (g.isNeighborVertex(wordId, words[i])) {
                break;
            }
            i++;
        }

        if (i == numWords) {
            return false;
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


    @Override
    public IntCollection getExtensibleWordIds() {
        return getExtensibleVertexIdsCount().keySet();
    }


    public HashIntIntMap getExtensibleVertexIdsCount() {
        ++total;
        if (numWords > 2) {
            int common = canDoIncremental(words, numWords);
            if (common > 1) {
                ++incremental;
//				System.out.println("Current vertiValueces:"+vertices+" previous Vertices:"+ Arrays.toString(previousVertices))
                return getExtensibleIdsIncrementalVertexIdsCount(words,
                        numWords,
                        common);
            }

            if (numWords == 0) {
                return getForZeroWords();
            }

            return getExtensibleIdsScratchVertexIdsCount(words, numWords);
        }

        if (numWords == 0) {
            return getForZeroWords();
        }
        return getExtensibleIdsScratchVertexIdsCount(words, numWords);

    }

    /**
     * Instead of starting empty, we are starting from the set that correspond to the
     * position that we have the last common with the previous.
     *
     * @param common
     * @return
     */
    HashIntIntMap getExtensibleIdsIncrementalVertexIdsCount(final int[] vertA,
            final int numVertices,
            final int common) {
        // This is faster than
//        extensionVertexIdMap = HashIntIntMaps.newMutableMap(incrementalVertexCount[common - 1]);
//        the following clearing and then adding.
        // Decided to use the following to limit the pressure on Garbage collection.
        // Should check with larger graphs...
        extensionVertexIdMap.clear();
        extensionVertexIdMap.putAll(incrementalVertexCount[common - 1]);

        previousWordsPos = numVertices;

        elementCounterConsumer.setMap(extensionVertexIdMap);

        for (int i = common; i < numVertices; i++) {
            final int vertexId = vertA[i];

            final IntCollection neighbourhood = getValidNeighboursForExpansion(vertexId);
            if (neighbourhood != null) {
                neighbourhood.forEach(elementCounterConsumer);
            }

            // We ignore the last one since it always changes!!!
            if (i < numVertices - 1) {
                growIncrementalVertexCountIfNeeded(i);
                if (incrementalVertexCount[i] == null) {
                    incrementalVertexCount[i] = HashIntIntMaps.newMutableMap(extensionVertexIdMap);
                } else {
                    incrementalVertexCount[i].clear();
                    incrementalVertexCount[i].putAll(extensionVertexIdMap);
                }
            }

            growPreviousVerticesIfNeeded(i);
            previousVertices[i] = vertexId;
        }

        // Clean the ones that exist already!!!.
        for (int i = 0; i < numWords; ++i) {
            int edgeId = words[i];
            extensionVertexIdMap.remove(edgeId);
        }

//		System.out.println("Incremental:"+vertices + "   " + extensionEdgeIds.size() + " common:"+common);
        return extensionVertexIdMap;
    }

    private void growIncrementalVertexCountIfNeeded(int i) {
        if (i >= incrementalVertexCount.length) {
            incrementalVertexCount = Arrays.copyOf(incrementalVertexCount, i + INC_ARRAY_SIZE);
        }
    }

    /**
     * @param vertA
     * @param numC
     * @return
     */
    HashIntIntMap getExtensibleIdsScratchVertexIdsCount(final int[] vertA,
            final int numC) {
        extensionVertexIdMap.clear();

        previousWordsPos = numC;
        elementCounterConsumer.setMap(extensionVertexIdMap);

        for (int i = 0; i < numC; i++) {
            final int vertexId = vertA[i];

            final IntCollection neighbourhood = getValidNeighboursForExpansion(vertexId);
            if (neighbourhood != null) {
                neighbourhood.forEach(elementCounterConsumer);
            }

            if (numC > 2) {
                growIncrementalVertexCountIfNeeded(i);
                //Prepare for incremental.
                if (incrementalVertexCount[i] == null) {
                    incrementalVertexCount[i] =
                            HashIntIntMaps.newMutableMap(extensionVertexIdMap);
                } else {
                    incrementalVertexCount[i].clear();
                    incrementalVertexCount[i].putAll(extensionVertexIdMap);
                }

                growPreviousVerticesIfNeeded(i);
                previousVertices[i] = vertexId;
            }
        }

        for (int i = 0; i < numWords; ++i) {
            int edgeId = words[i];
            extensionVertexIdMap.remove(edgeId);
        }
// 		System.out.println("Scratch" + Arrays.toString(vertA) + "   " + extensionEdgeIds.size());
        return extensionVertexIdMap;
    }

    private void growPreviousVerticesIfNeeded(int i) {
        if (i >= previousVertices.length) {
            previousVertices = Arrays.copyOf(previousVertices, i + INC_ARRAY_SIZE);
        }
    }

    protected IntCollection getValidNeighboursForExpansion(int vertexId) {
        return g.getVertexNeighbours(vertexId);
    }


    /**
     * @return
     */
    private HashIntIntMap getForZeroWords() {
        extensionVertexIdMap.clear();
        final int numVertices = g.getNumberVertices();

        for (int i = 0; i < numVertices; i++) {
            extensionVertexIdMap.put(i, 0);
        }

        return extensionVertexIdMap;
    }

    @Override
    public void addWord(int word) {
        super.addWord(word);
        _updateEdges(word);
    }

    @Override
    public void removeLastWord() {
        if (numWords > 0) {
            edgesSize -= numAdded[numWords - 1];
        }

        super.removeLastWord();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        _updateEdges();
    }

    public int getNumEdges() {
        return edgesSize;
    }

    private void _updateEdges(int srcVertexId) {
        int addedEdges = 0;

        //We exclude itself (the last one added).
        for (int i = 0; i < numWords - 1; ++i) {
            int dstVertexId = words[i];
            ReclaimableIntCollection edgeIds = g.getEdgeIds(srcVertexId, dstVertexId);

            if (edgeIds != null) {
                if (edgesSize + edgeIds.size() >= edges.length) {
                    edges = Arrays.copyOf(edges, edgesSize + edgeIds.size() + INC_ARRAY_SIZE);
                }

                updateEdgesConsumer.reset();
                edgeIds.forEach(updateEdgesConsumer);
                addedEdges += updateEdgesConsumer.getNumAdded();
                edgeIds.reclaim();
            }
        }
        if (numAdded.length < numWords) {
            numAdded = Arrays.copyOf(numAdded, numWords + INC_ARRAY_SIZE);
        }
        numAdded[numWords - 1] = addedEdges;
    }

    private void _updateEdges() {
        edgesSize = 0;
        /*for (int i = 0; i < numWords - 1; ++i) {
            int srcVertexId = words[i];

            for (int j = i + 1; j < numWords; ++j) {
                int dstVertexId = words[j];

                int edgeId = g.getEdgeId(srcVertexId, dstVertexId);

                if (edgeId != -1) {
                    if (edgesSize >= edges.length) {
                        edges = Arrays.copyOf(edges, edgesSize + INC_ARRAY_SIZE);
                    }

                    edges[edgesSize++] = edgeId;
                }
            }
        }*/

        for (int i = 1; i < numWords; ++i) {
            int dstVertexId = words[i];

            for (int j = 0; j < i; ++j) {
                int srcVertexId = words[j];

                ReclaimableIntCollection edgeIds = g.getEdgeIds(srcVertexId, dstVertexId);

                if (edgeIds != null) {
                    if (edgesSize + edgeIds.size() >= edges.length) {
                        edges = Arrays.copyOf(edges, edgesSize + edgeIds.size() + INC_ARRAY_SIZE);
                    }

                    updateEdgesConsumer.reset();
                    edgeIds.forEach(updateEdgesConsumer);
                    edgeIds.reclaim();
                }
            }
        }

        int lastWord = words[numWords - 1];
        int addedEdges = 0;

        //We exclude itself (the last one added).
        for (int i = 0; i < numWords - 1; ++i) {
            int dstVertexId = words[i];
            ReclaimableIntCollection edgeIds = g.getEdgeIds(lastWord, dstVertexId);

            if (edgeIds != null) {
                addedEdges += edgeIds.size();
                edgeIds.reclaim();
            }
        }

        if (numAdded.length < numWords) {
            numAdded = Arrays.copyOf(numAdded, numWords + INC_ARRAY_SIZE);
        }
        numAdded[numWords - 1] = addedEdges;
    }

    public int[] getEdges() {
        return edges;
    }

    private class UpdateEdgesConsumer implements IntConsumer {
        private int numAdded;

        public void reset() {
            numAdded = 0;
        }

        public int getNumAdded() {
            return numAdded;
        }

        @Override
        public void accept(int i) {
            edges[edgesSize++] = i;
            ++numAdded;
        }
    }
}
