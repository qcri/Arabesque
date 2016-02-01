package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import org.apache.hadoop.io.Writable;

public interface CommonMasterExecutionEngine {

    long getSuperstep();

    void haltComputation();

}
