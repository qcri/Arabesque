package io.arabesque.computation;

import io.arabesque.embedding.EdgeInducedEmbedding;

public abstract class EdgeInducedComputation<E extends EdgeInducedEmbedding> extends BasicComputation<E> {
    @Override
    protected final int getInitialNumWords() {
        return getMainGraph().getNumberEdges();
    }

    @Override
    public E createEmbedding() {
        return (E) new EdgeInducedEmbedding();
    }
}
