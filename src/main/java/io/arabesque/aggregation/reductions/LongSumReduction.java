package io.arabesque.aggregation.reductions;

import org.apache.hadoop.io.LongWritable;

public class LongSumReduction implements ReductionFunction<LongWritable> {
    @Override
    public LongWritable reduce(LongWritable k1, LongWritable k2) {
        if (k1 != null && k2 != null) {
            k1.set(k1.get() + k2.get());
        }

        return k1;
    }
}
