package io.arabesque.computation.comm;

import io.arabesque.embedding.Embedding;
import io.arabesque.odag.ODAGPartLZ4Wrapper;
import org.apache.hadoop.io.Writable;

/**
 * Created by Alex on 20-Sep-15.
 */
public class ODAGCommunicationStrategyFactory implements CommunicationStrategyFactory {
    @Override
    public <O extends Embedding> CommunicationStrategy<O> createCommunicationStrategy() {
        return new ODAGCommunicationStrategy<>();
    }

    @Override
    public <M extends Writable> M createMessage() {
        return (M) new ODAGPartLZ4Wrapper();
    }
}
