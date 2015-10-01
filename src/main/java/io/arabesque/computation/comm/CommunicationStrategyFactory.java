package io.arabesque.computation.comm;

import io.arabesque.embedding.Embedding;
import org.apache.hadoop.io.Writable;

public interface CommunicationStrategyFactory {
    <O extends Embedding> CommunicationStrategy<O> createCommunicationStrategy();

    <M extends Writable> M createMessage();
}
