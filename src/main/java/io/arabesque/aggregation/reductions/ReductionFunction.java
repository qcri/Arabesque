package io.arabesque.aggregation.reductions;

import org.apache.hadoop.io.Writable;

import java.io.Serializable;

public abstract class ReductionFunction<V extends Writable> implements Serializable {
    public abstract V reduce(V v1, V v2);
}
