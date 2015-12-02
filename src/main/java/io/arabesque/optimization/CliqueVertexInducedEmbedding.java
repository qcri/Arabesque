package io.arabesque.optimization;

import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;

public class CliqueVertexInducedEmbedding extends VertexInducedEmbedding {
    @Override
    public IntCollection getExtensibleWordIds() {
        if (dirtyExtensionWordIds) {
            extensionWordIds.clear();

            IntCollection lastVertexNeighbours = mainGraph.getVertexNeighbours(getVertices().getLast());

            if (lastVertexNeighbours != null) {
                extensionWordIds.addAll(lastVertexNeighbours);
            }

            int numVertices = getNumVertices();
            IntArrayList vertices = getVertices();

            // Clean the words that are already in the embedding
            for (int i = 0; i < numVertices; ++i) {
                int wId = vertices.getUnchecked(i);
                extensionWordIds.removeInt(wId);
            }
        }

        return extensionWordIds;
    }

    @Override
    public boolean isCanonicalEmbeddingWithWord(int wordId) {
        if (this.getNumVertices() == 0) return true;

        return wordId > getVertices().getUnchecked(getNumVertices() - 1);
    }
}
