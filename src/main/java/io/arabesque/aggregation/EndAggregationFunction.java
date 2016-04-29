package io.arabesque.aggregation;

import org.apache.hadoop.io.Writable;

import java.io.Serializable;

public interface EndAggregationFunction<K extends Writable, V extends Writable> extends Serializable {
    void endAggregation(AggregationStorage<K, V> aggregationStorage);
}
