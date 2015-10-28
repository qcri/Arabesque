package io.arabesque.aggregation.reductions;

import org.apache.hadoop.io.Writable;

public interface ReductionFunction<V extends Writable> {
    V reduce(V v1, V v2);
}
