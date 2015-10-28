package io.arabesque.optimization;

import io.arabesque.embedding.VertexInducedEmbedding;
import net.openhft.koloboke.collect.IntCollection;

public class CliqueVertexInducedEmbedding extends VertexInducedEmbedding {
    @Override
    public IntCollection getExtensibleWordIds() {
        return g.getVertexNeighbours(getVertices()[getNumVertices() - 1]);
    }

    @Override
    public boolean isCanonicalEmbeddingWithWord(int wordId) {
        if (this.numWords == 0) return true;

        return wordId > getVertices()[getNumVertices() - 1];
    }
}
