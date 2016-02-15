package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import org.apache.hadoop.io.Writable;

public class MasterComputation {
    private CommonMasterExecutionEngine executionEngine;

    public void init() {
        // Do nothing by default
    }

    public void compute() {
        // Do nothing by default
    }

    public int getStep() {
        return (int) executionEngine.getSuperstep();
    }

    public void haltComputation() {
        executionEngine.haltComputation();
    }

    public <K extends Writable, V extends Writable> AggregationStorage<K, V> readAggregation(String name) {
        return executionEngine.getAggregatedValue(name);
    }


    public <K extends Writable, V extends Writable> void setAggregation(String name, AggregationStorage<K, V> aggregation) {
        executionEngine.setAggregatedValue(name, aggregation);
    }

    public void setUnderlyingExecutionEngine(CommonMasterExecutionEngine executionEngine) {
        this.executionEngine = executionEngine;
    }
}
