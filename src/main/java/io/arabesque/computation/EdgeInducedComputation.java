package io.arabesque.computation;

import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;

public abstract class EdgeInducedComputation<E extends EdgeInducedEmbedding> extends BasicComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberEdges();
    }

    @Override
    public Class<? extends Embedding> getEmbeddingClass() {
        return EdgeInducedEmbedding.class;
    }
}
