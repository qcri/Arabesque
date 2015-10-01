package io.arabesque.aggregation;

import org.apache.hadoop.io.Writable;

public interface EndAggregationFunction<K extends Writable, V extends Writable> {
    void endAggregation(AggregationStorage<K, V> aggregationStorage);
}
