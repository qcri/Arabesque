package io.arabesque.embedding;

import io.arabesque.graph.Vertex;
import io.arabesque.utils.ElementCounterConsumer;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

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

    public VertexInducedEmbedding() {
        super();
        edges = new int[INC_ARRAY_SIZE];
        extensionVertexIds = HashIntSets.newMutableSet();
        extensionVertexIdMap = HashIntIntMaps.newMutableMap();

        edges = new int[INC_ARRAY_SIZE];
        numAdded = new int[INC_ARRAY_SIZE];
        incrementalVertexCount = new HashIntIntMap[INC_ARRAY_SIZE];
        elementCounterConsumer = new ElementCounterConsumer();
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
        return 1;
    }

    @Override
    public int getNumEdgesAddedWithExpansion() {
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
                if (incrementalVertexCount[i] == null) {
                    incrementalVertexCount[i] = HashIntIntMaps.newMutableMap(extensionVertexIdMap);
                } else {
                    incrementalVertexCount[i].clear();
                    incrementalVertexCount[i].putAll(extensionVertexIdMap);
                }
            }
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
                //Prepare for incremental.
                if (incrementalVertexCount[i] == null) {
                    incrementalVertexCount[i] =
                            HashIntIntMaps.newMutableMap(extensionVertexIdMap);
                } else {
                    incrementalVertexCount[i].clear();
                    incrementalVertexCount[i].putAll(extensionVertexIdMap);
                }

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
            int edgeId = g.getEdgeId(srcVertexId, dstVertexId);

            if (edgeId != -1) {
                if (edgesSize >= edges.length) {
                    edges = Arrays.copyOf(edges, edgesSize + INC_ARRAY_SIZE);
                    System.out.println("Increasing edges size!!!");
                }
                edges[edgesSize++] = edgeId;
                ++addedEdges;
            }
        }
        if (numAdded.length < numWords) {
            numAdded = Arrays.copyOf(numAdded, numAdded.length + INC_ARRAY_SIZE);
        }
        numAdded[numWords - 1] = addedEdges;
    }

    private void _updateEdges() {
        edgesSize = 0;
        for (int i = 0; i < numWords; ++i) {
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
        }
    }

    public int[] getEdges() {
        return edges;
    }
}
