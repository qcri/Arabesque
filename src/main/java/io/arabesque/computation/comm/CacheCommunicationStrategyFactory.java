package io.arabesque.computation.comm;

import io.arabesque.cache.LZ4ObjectCache;
import io.arabesque.embedding.Embedding;
import org.apache.hadoop.io.Writable;

public class CacheCommunicationStrategyFactory implements CommunicationStrategyFactory {
    @Override
    public <O extends Embedding> CommunicationStrategy<O> createCommunicationStrategy() {
        return new CacheCommunicationStrategy<>();
    }

    @Override
    public <M extends Writable> M createMessage() {
        return (M) new LZ4ObjectCache();
    }
}
