package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.embedding.Embedding;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import org.apache.hadoop.io.Writable;

public interface Computation<E extends Embedding> {
    // {{{ Initialization and finish hooks
    void init();

    void initAggregations();

    void finish();
    // }}}

    // {{{ Filter-Process model
    boolean filter(E embedding);

    void process(E embedding);

    boolean aggregationFilter(E Embedding);

    void aggregationProcess(E embedding);

    void handleNoExpansions(E embedding);

    boolean shouldExpand(E newEmbedding);
    // }}}

    // {{{ Other filter-hooks (performance/canonicality related)
    void filter(E existingEmbedding, HashIntSet extensionPoints);

    boolean filter(E existingEmbedding, int newWord);
    // }}}

    // {{{ Output
    void output(E embedding);
    // }}}

    // {{{ Aggregation-related stuff
    <K extends Writable, V extends Writable> AggregationStorage<K, V> readAggregation(String name);

    <K extends Writable, V extends Writable> AggregationStorage<K, V> readInterstepAggregation();

    <K extends Writable, V extends Writable> AggregationStorage<K, V> readOutputAggregation();

    <K extends Writable, V extends Writable> void map(String name, K key, V value);

    <K extends Writable, V extends Writable> void mapInterstep(K key, V value);

    <K extends Writable, V extends Writable> void mapOutput(K key, V value);
    // }}}

    // {{{ Misc
    int getStep();

    int getPartitionId();

    int getNumberPartitions();
    // }}}

    // {{{ Internal
    void setUnderlyingExecutionEngine(ExecutionEngine<E> executionEngine);

    void expand(E embedding);

    E createEmbedding();
    // }}}
}
