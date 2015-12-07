package io.arabesque.computation.comm;

import io.arabesque.cache.LZ4ObjectCache;
import io.arabesque.computation.BasicComputation;
import io.arabesque.computation.Computation;
import io.arabesque.computation.MasterExecutionEngine;
import io.arabesque.computation.WorkerContext;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Iterator;

public class CacheCommunicationStrategy<O extends Embedding> extends CommunicationStrategy<O> {
    private String partitionCounterKey;
    private String groupCounterKey;
    private long newIdCounter;
    private long newGroupCounter;
    private LZ4ObjectCache[] outputCaches;
    private IntWritable reusableDestinationId;
    private MessageWrapper reusableMessageWrapper;
    private Iterator<MessageWrapper> messageIterator;
    LZ4ObjectCache currentObjectCache;
    private long totalSizeEmbeddingsProcessed;
    private int numberOfPartitions;

    private boolean patternAggFilterDefined;
    private Computation computation;

    @Override
    public void initialize(int phase) {
        super.initialize(phase);

        WorkerContext workerContext = getWorkerContext();

        numberOfPartitions = getWorkerContext().getNumberPartitions();

        reusableDestinationId = new IntWritable();
        reusableMessageWrapper = new MessageWrapper();

        int partitionId = getExecutionEngine().getPartitionId();
        partitionCounterKey = "newIdCounter" + partitionId;
        groupCounterKey = "groupCounter" + partitionId;

        newIdCounter = workerContext.getLongData(partitionCounterKey);
        newGroupCounter = workerContext.getLongData(groupCounterKey);

        if (newIdCounter == -1) {
            long countersPerPartition = Long.MAX_VALUE / numberOfPartitions;

            newIdCounter = countersPerPartition * partitionId;
            newGroupCounter = countersPerPartition * partitionId;

            String partitionCounterMaxKey = "newIdCounterMax" + partitionId;
            long maxCounterValueForPartition = newIdCounter + countersPerPartition - 1;
            workerContext.setData(partitionCounterMaxKey, maxCounterValueForPartition);
        }

        outputCaches = new LZ4ObjectCache[numberOfPartitions];

        for (int i = 0; i < outputCaches.length; ++i) {
            outputCaches[i] = new LZ4ObjectCache();
        }

        totalSizeEmbeddingsProcessed = 0;
    }

    @Override
    public int getNumPhases() {
        return 1;
    }

    @Override
    public void flush() {
        for (int i = 0; i < outputCaches.length; ++i) {
            LZ4ObjectCache cache = outputCaches[i];
            reusableDestinationId.set(i);
            flushCache(i, cache);
        }
    }

    private void flushCache(int partitionId, LZ4ObjectCache outputCache) {
        if (!outputCache.isEmpty()) {
            reusableDestinationId.set(partitionId);
            reusableMessageWrapper.setMessage(outputCache);
            sendMessage(reusableDestinationId, reusableMessageWrapper);
            outputCache.reset();
        }
    }

    @Override
    public void finish() {
        flush();

        WorkerContext workerContext = getWorkerContext();

        workerContext.setData(partitionCounterKey, newIdCounter);
        workerContext.setData(groupCounterKey, newGroupCounter);

        LongWritable longWritable = new LongWritable();
        longWritable.set(totalSizeEmbeddingsProcessed);

        getExecutionEngine().aggregate(MasterExecutionEngine.AGG_PROCESSED_SIZE_CACHE, longWritable);
    }

    @Override
    public void startComputation(Vertex<IntWritable, NullWritable, NullWritable> vertex, Iterable<MessageWrapper> messages) {
        super.startComputation(vertex, messages);

        messageIterator = messages.iterator();

        computation = getExecutionEngine().getComputation();
        try {
            patternAggFilterDefined = computation.getClass().getMethod("aggregationFilter", Pattern.class).getDeclaringClass() != BasicComputation.class;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public O getNextInboundEmbedding() {
        if (currentObjectCache == null) {
            if (messageIterator.hasNext()) {
                currentObjectCache = messageIterator.next().getMessage();
            }

            if (currentObjectCache == null) {
                return null;
            } else {
                currentObjectCache.prepareForIteration();
            }
        }

        while (currentObjectCache.hasNext()) {
            O embedding = (O) currentObjectCache.next();

            if (!patternAggFilterDefined || computation.aggregationFilter(embedding.getPattern())) {
                return embedding;
            }
        }

        totalSizeEmbeddingsProcessed += currentObjectCache.getByteArrayOutputCache().getPos();

        currentObjectCache = null;

        return getNextInboundEmbedding();
    }

    @Override
    public void addOutboundEmbedding(O expansion) {
        int destinationPartition = (int) ((newIdCounter++) % numberOfPartitions);

        LZ4ObjectCache outputCache = outputCaches[destinationPartition];

        try {
            outputCache.addObject(expansion);
        } catch (IOException e) {
            throw new RuntimeException("Unable to add outbound embedding", e);
        }

        if (outputCache.overThreshold()) {
            flushCache(destinationPartition, outputCache);
        }
    }
}
