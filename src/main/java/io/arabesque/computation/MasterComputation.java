package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.AggregationStorageWrapper;
import io.arabesque.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class MasterComputation {
    private MasterExecutionEngine executionEngine;

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

    public <K extends Writable, V extends Writable> AggregationStorage<K, V> readInterstepAggregation() {
        return readAggregation(Configuration.AGG_INTERSTEP);
    }

    public <K extends Writable, V extends Writable> AggregationStorage<K, V> readOutputAggregation() {
        return readAggregation(Configuration.AGG_OUTPUT);
    }

    public <K extends Writable, V extends Writable> void setAggregation(String name, AggregationStorage<K, V> aggregation) {
        executionEngine.setAggregatedValue(name, aggregation);
    }

    public <K extends Writable, V extends Writable> void setInterstepAggregation(AggregationStorage<K, V> aggregation) {
        setAggregation(Configuration.AGG_INTERSTEP, aggregation);
    }

    public <K extends Writable, V extends Writable> void setOutputAggregation(AggregationStorage<K, V> aggregation) {
        setAggregation(Configuration.AGG_OUTPUT, aggregation);
    }

    public void setUnderlyingExecutionEngine(MasterExecutionEngine executionEngine) {
        this.executionEngine = executionEngine;
    }
}
