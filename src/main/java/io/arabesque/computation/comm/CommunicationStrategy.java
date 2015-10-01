package io.arabesque.computation.comm;

import io.arabesque.computation.ExecutionEngine;
import io.arabesque.computation.WorkerContext;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

public abstract class CommunicationStrategy<O extends Embedding> {
    private ExecutionEngine<O> executionEngine;
    private WorkerContext workerContext;
    private Configuration<O> configuration;
    private int currentPhase;
    private Vertex<IntWritable, NullWritable, NullWritable> currentVertex;

    public CommunicationStrategy() {
        this.currentPhase = 0;
    }

    public void setExecutionEngine(ExecutionEngine<O> executionEngine) {
        this.executionEngine = executionEngine;
    }

    public void setWorkerContext(WorkerContext workerContext) {
        this.workerContext = workerContext;
    }

    public void setConfiguration(Configuration<O> configuration) {
        this.configuration = configuration;
    }

    public ExecutionEngine<O> getExecutionEngine() {
        return executionEngine;
    }

    public WorkerContext getWorkerContext() {
        return workerContext;
    }

    public Configuration<O> getConfiguration() {
        return configuration;
    }

    public abstract int getNumPhases();

    public int getCurrentPhase() {
        return currentPhase;
    }

    public Vertex<IntWritable, NullWritable, NullWritable> getCurrentVertex() {
        return currentVertex;
    }

    public void initialize(int phase) {
        currentPhase = phase;
    }

    public abstract void flush();

    public abstract void finish();

    public abstract O getNextInboundEmbedding();

    protected void sendMessage(IntWritable destinationId, MessageWrapper message) {
        executionEngine.sendMessage(destinationId, message);
    }

    public void startComputation(Vertex<IntWritable, NullWritable, NullWritable> vertex,
            Iterable<MessageWrapper> messages) {
        currentVertex = vertex;
    }

    public void endComputation() {
        // Empty by design
    }

    public abstract void addOutboundEmbedding(O expansion);

}
