package io.arabesque.computation;

import org.apache.hadoop.io.Writable;

public interface CommonMasterExecutionEngine {

    long getSuperstep();

    void haltComputation();
    
    public <A extends Writable> A getAggregatedValue(String name);
    
    public <A extends Writable> void setAggregatedValue(String name, A value);

}
