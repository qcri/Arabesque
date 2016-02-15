package io.arabesque.aggregation.reductions;

import org.apache.hadoop.io.Writable;
import java.io.Serializable;

public interface ReductionFunction<V extends Writable> extends Serializable {
    V reduce(V v1, V v2);
}
