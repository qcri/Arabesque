package io.arabesque.computation;

import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;

/**
 * Created by siganos on 8/29/16.
 */
public abstract class SubsetComputation<E extends VertexInducedEmbedding> extends BasicComputation<E> {

    @Override
    protected int getInitialNumWords() {
        IntArrayList partialVertices = underlyingExecutionEngine.getPartialVertices();
        if (partialVertices==null) {
            return getMainGraph().getNumberVertices();
        }
        return partialVertices.getSize();
    }

    @Override
    protected HashIntSet getInitialExtensions() {
        IntArrayList partialVertices = underlyingExecutionEngine.getPartialVertices();
        if (partialVertices==null) {
            return super.getInitialExtensions();
        }

        // We only do some of the vertices not all.
        int totalNumWords = partialVertices.getSize();
        int numPartitions = getNumberPartitions();
        int myPartitionId = getPartitionId();
        int numWordsPerPartition = Math.max(totalNumWords / numPartitions, 1);
        int startMyWordRange = myPartitionId * numWordsPerPartition;
        int endMyWordRange = startMyWordRange + numWordsPerPartition;

        // If we are the last partition or our range end goes over the total number
        // of vertices, set the range end to the total number of vertices.
        if (myPartitionId == numPartitions - 1 || endMyWordRange > totalNumWords) {
            endMyWordRange = totalNumWords;
        }

        // TODO: Replace this by a list implementing IntCollection. No need for set.
        HashIntSet initialExtensions = HashIntSets.newMutableSet(numWordsPerPartition);

        for (int i = startMyWordRange; i < endMyWordRange; ++i) {
            initialExtensions.add(partialVertices.getUnchecked(i));
        }
        return initialExtensions;
    }

    public Class<? extends Embedding> getEmbeddingClass() {
        return VertexInducedEmbedding.class;
    }

}

