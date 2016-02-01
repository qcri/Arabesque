package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.AggregationStorageFactory;
import io.arabesque.aggregation.AggregationStorageMetadata;
import io.arabesque.computation.comm.CommunicationStrategy;
import io.arabesque.computation.comm.MessageWrapper;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public interface CommonExecutionEngine<O extends Embedding> {

    void processExpansion(O expansion);

    <A extends Writable> A getAggregatedValue(String name);

    <K extends Writable, V extends Writable> void map(String name, K key, V value);
    
    int getPartitionId();

    int getNumberPartitions();

    long getSuperstep();

    void aggregate(String name, LongWritable value);
    
    void output(String outputString);

}
