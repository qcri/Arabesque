package io.arabesque.embedding;

import io.arabesque.graph.Edge;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;

import java.io.DataInput;
import java.io.IOException;

public class EdgeInducedEmbedding extends BasicEmbedding {
    private IntArrayList numVerticesAddedWithWord;

    @Override
    public void init() {
        numVerticesAddedWithWord = new IntArrayList();

        super.init();
    }

    @Override
    public void reset() {
        super.reset();
        numVerticesAddedWithWord.clear();
    }

    @Override
    public IntArrayList getWords() {
        return getEdges();
    }

    @Override
    public int getNumWords() {
        return getNumEdges();
    }

    @Override
    public String toOutputString() {
        StringBuilder sb = new StringBuilder();

        int numEdges = getNumEdges();
        IntArrayList edges = getEdges();

        for (int i = 0; i < numEdges; ++i) {
            Edge edge = mainGraph.getEdge(edges.getUnchecked(i));
            sb.append(edge.getSourceId());
            sb.append("-");
            sb.append(edge.getDestinationId());
            sb.append(" ");
        }

        return sb.toString();
    }

    @Override
    public int getNumVerticesAddedWithExpansion() {
        return numVerticesAddedWithWord.getLastOrDefault(0);
    }

    @Override
    public int getNumEdgesAddedWithExpansion() {
        if (edges.isEmpty()) {
            return 0;
        }

        return 1;
    }

    @Override
    protected IntCollection getValidNeighboursForExpansion(int vertexId) {
        return mainGraph.getVertexNeighbourhood(vertexId).getNeighbourEdges();
    }

    @Override
    protected boolean areWordsNeighbours(int wordId1, int wordId2) {
        return mainGraph.areEdgesNeighbors(wordId1, wordId2);
    }


    /**
     * Add word and update the number of vertices in this embedding.
     *
     * @param word
     */
    @Override
    public void addWord(int word) {
        super.addWord(word);
        edges.add(word);
        updateVertices(word);
    }

    private void updateVertices(int word) {
        final Edge edge = mainGraph.getEdge(word);

        int numVerticesAdded = 0;

        boolean srcIsNew = false;
        boolean dstIsNew = false;

        if (!vertices.contains(edge.getSourceId())) {
            srcIsNew = true;
        }

        if (!vertices.contains(edge.getDestinationId())) {
            dstIsNew = true;
        }

        if (srcIsNew) {
            vertices.add(edge.getSourceId());
            ++numVerticesAdded;
        }

        if (dstIsNew) {
            vertices.add(edge.getDestinationId());
            ++numVerticesAdded;
        }

        numVerticesAddedWithWord.add(numVerticesAdded);
    }

    @Override
    public void removeLastWord() {
        if (getNumEdges() == 0) {
            return;
        }

        int numVerticesToRemove = numVerticesAddedWithWord.pop();
        vertices.removeLast(numVerticesToRemove);
        edges.removeLast();

        super.removeLastWord();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();

        edges.readFields(in);

        int numEdges = edges.size();

        for (int i = 0; i < numEdges; ++i) {
            updateVertices(edges.getUnchecked(i));
        }
    }
}
