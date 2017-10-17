package io.arabesque.embedding;

import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.function.IntConsumer;

import java.io.DataInput;
import java.io.IOException;
import java.io.ObjectInput;

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
            // TODO hack! this is because sometimes we invert the edge id to encode information
            int id = Math.abs(edges.getUnchecked(i));
            sb.append(id).append("-").append(mainGraph.getEdgeDst(id)).append("-").append(mainGraph.getEdgeLabel(id));
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
    protected void processValidNeighbors(int vertexId, IntConsumer intAddConsumer) {
        mainGraph.processEdgeNeighbors(vertexId, intAddConsumer);
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
        // TODO hack! this is because sometimes we invert the edge id to encode information
        final int srcId = mainGraph.getEdgeSource(word);
        final int dstId = mainGraph.getEdgeDst(word);

        int numVerticesAdded = 0;

        if (!vertices.contains(srcId)) {
            vertices.add(srcId);
            ++numVerticesAdded;
        }

        if (!vertices.contains(dstId)) {
            vertices.add(dstId);
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

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields(objInput);
    }
}
