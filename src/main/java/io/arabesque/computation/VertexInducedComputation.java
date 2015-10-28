package io.arabesque.computation;

import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;

public abstract class VertexInducedComputation<E extends VertexInducedEmbedding> extends BasicComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberVertices();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return VertexInducedEmbedding.class;
    }
}
