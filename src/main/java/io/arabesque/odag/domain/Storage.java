package io.arabesque.odag.domain;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.Writable;

public abstract class Storage<S extends Storage> implements Writable {
    public abstract void addEmbedding(Embedding embedding);

    public abstract void aggregate(S storage);

    public abstract long getNumberOfEnumerations();

    public abstract void clear();

    public abstract StorageStats getStats();

    public abstract String toStringResume();

    public abstract StorageReader getReader(Pattern pattern, Computation<Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize);
}

