package io.arabesque.aggregation.reductions;

import org.apache.hadoop.io.Writable;
import java.io.Serializable;

import org.apache.spark.api.java.function.Function2;

public abstract class ReductionFunction<V extends Writable> implements Serializable {
    public abstract V reduce(V v1, V v2);
   
    // in order to be called from spark operators:
    // e.g. rdd.reduceByKey (this.func.call)
    public final Function2<V, V, V> func = new Function2<V, V, V>() {
       public V call(V v1, V v2) {
          return reduce(v1, v2);
       }
    };
}
