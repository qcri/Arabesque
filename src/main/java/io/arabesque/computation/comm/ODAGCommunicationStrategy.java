package io.arabesque.computation.comm;

import io.arabesque.computation.Computation;
import io.arabesque.computation.MasterExecutionEngine;
import io.arabesque.computation.WorkerContext;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.ODAG;
import io.arabesque.odag.ODAGPartLZ4Wrapper;
import io.arabesque.odag.ODAGStash;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ODAGCommunicationStrategy<O extends Embedding> extends CommunicationStrategy<O> {
    private boolean splitEzips = true;
    private int numberEzipAggregators = 0;
    private int numBlocks;
    private int maxBlockSize;
    private IntWritable reusableDestinationId;
    private MessageWrapper reusableMessageWrapper;
    private ODAGPartLZ4Wrapper reusablePartWrapper;
    private ExtendedByteArrayDataOutput[] dataOutputs;
    private boolean[] hasContent;

    private ODAGStash currentEmbeddingStash;
    private ODAGStash nextEmbeddingStash;
    private ODAGStash aggregatedEmbeddingStash;
    private ODAGStash.EfficientReader<O> odagStashReader;

    private ODAGLocalCoordinationObjectFactory coordinationObjectFactory;
    private long totalSizeODAGs;

    public ODAGCommunicationStrategy() {
        coordinationObjectFactory = new ODAGLocalCoordinationObjectFactory(this);
    }

    @Override
    public int getNumPhases() {
        return 2;
    }

    @Override
    public void initialize(int phase) {
        super.initialize(phase);

        nextEmbeddingStash = new ODAGStash();

        splitEzips = Configuration.get().getBoolean("splitEzips", true);

        WorkerContext workerContext = getWorkerContext();

        numberEzipAggregators = Configuration.get().getODAGNumAggregators();

        if (numberEzipAggregators == -1) {
            // TODO: Figure out a better default?
            numberEzipAggregators = workerContext.getWorkerCount();
        }

        workerContext.setupLocalCoordination(coordinationObjectFactory);

        int numPartitions = workerContext.getNumberPartitions();
        numBlocks = Configuration.get().getInteger("numBlocks", numPartitions * numPartitions);
        maxBlockSize = Configuration.get().getInteger("maxBlockSize", 10000);

        reusableDestinationId = new IntWritable();
        reusablePartWrapper = new ODAGPartLZ4Wrapper();
        reusableMessageWrapper = new MessageWrapper(reusablePartWrapper);

        dataOutputs = new ExtendedByteArrayDataOutput[numberEzipAggregators];

        for (int i = 0; i < dataOutputs.length; ++i) {
            dataOutputs[i] = new ExtendedByteArrayDataOutput();
        }

        hasContent = new boolean[numberEzipAggregators];
    }

    @Override
    public void flush() {
        if (getCurrentPhase() == 0) {
            if (nextEmbeddingStash == null) {
                return;
            }

            Collection<ODAG> ezips = nextEmbeddingStash.getEzips();
            Iterator<ODAG> ezipsIterator = ezips.iterator();

            while (ezipsIterator.hasNext()) {
                ODAG ezip = ezipsIterator.next();

                if (splitEzips) {
                    Arrays.fill(hasContent, false);

                    for (int i = 0; i < numberEzipAggregators; ++i) {
                        dataOutputs[i].reset();
                    }

                    try {
                        ezip.writeInParts(dataOutputs, hasContent);
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to write ezip in parts", e);
                    }

                    for (int i = 0; i < numberEzipAggregators; ++i) {
                        if (!hasContent[i]) {
                            continue;
                        }

                        reusableDestinationId.set(i);
                        reusablePartWrapper.reset();
                        reusablePartWrapper.setPattern(ezip.getPattern());
                        reusablePartWrapper.setPartId(i);
                        reusablePartWrapper.setByteArrayOutputCache(dataOutputs[i]);

                        sendMessage(reusableDestinationId, reusableMessageWrapper);
                        reusablePartWrapper.setByteArrayOutputCache(null);
                    }
                } else {
                    int patternHash = ezip.getPattern().hashCode();
                    int destinationId = patternHash % numberEzipAggregators;

                    if (destinationId < 0) {
                        destinationId += numberEzipAggregators;
                    }

                    reusableDestinationId.set(destinationId);
                    reusablePartWrapper.reset();
                    try {
                        reusablePartWrapper.writeEzip(ezip, getCurrentVertex().getId().get());
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to write ezip to wrapper", e);
                    }
                    sendMessage(reusableDestinationId, reusableMessageWrapper);
                }

                ezipsIterator.remove();
            }

            nextEmbeddingStash.clear();
        } else if (getCurrentPhase() == 1) {
            if (aggregatedEmbeddingStash == null) {
                return;
            }

            Collection<ODAG> ezips = aggregatedEmbeddingStash.getEzips();
            Iterator<ODAG> ezipsIterator = ezips.iterator();

            while (ezipsIterator.hasNext()) {
                ODAG ezip = ezipsIterator.next();

                // TODO: Pool this?
                ODAGPartLZ4Wrapper nonReusableWrapper = new ODAGPartLZ4Wrapper();

                try {
                    nonReusableWrapper.writeEzip(ezip, getCurrentVertex().getId().get());
                    nonReusableWrapper.ensureCompressed();
                } catch (IOException e) {
                    throw new RuntimeException("Unable to write ezip to wrapper", e);
                }

                try {
                    getWorkerContext().broadcast(nonReusableWrapper);
                } catch (IOException e) {
                    throw new RuntimeException("Broadcast error", e);
                }

                ezipsIterator.remove();
            }
        }
    }

    @Override
    public void startComputation(Vertex<IntWritable, NullWritable, NullWritable> vertex,
            Iterable<MessageWrapper> messages) {
        super.startComputation(vertex, messages);

        if (getCurrentPhase() == 1) {
            aggregatedEmbeddingStash = new ODAGStash();

            ODAG ezip = new ODAG(false);

            for (MessageWrapper messageWrapper : messages) {
                ODAGPartLZ4Wrapper ezipWrapper = messageWrapper.getMessage();
                ezipWrapper.readEzip(ezip);

                aggregatedEmbeddingStash.aggregateUsingReusable(ezip);
            }
        }
    }

    @Override
    public void finish() {
        flush();

        LongWritable longWritable = new LongWritable();
        longWritable.set(totalSizeODAGs);

        getExecutionEngine().aggregate(MasterExecutionEngine.AGG_PROCESSED_SIZE_ODAG, longWritable);
    }

    @Override
    public O getNextInboundEmbedding() {
        if (currentEmbeddingStash == null) {
            ODAGLocalCoordinationObject coordinationObject =
                    (ODAGLocalCoordinationObject) getWorkerContext().accessLocalCoordinationObject();

            currentEmbeddingStash = coordinationObject.getCurrentUnserializedStash();

            if (currentEmbeddingStash == null) {
                totalSizeODAGs = coordinationObject.getTotalSizeODAGs().get();
                return null;
            }

            odagStashReader = new ODAGStash.EfficientReader<>(
                    currentEmbeddingStash, getExecutionEngine().getComputation(),
                    getWorkerContext().getNumberPartitions(), numBlocks, maxBlockSize);
        }

        if (odagStashReader.hasNext()) {
            return odagStashReader.next();
        } else {
            currentEmbeddingStash = null;
            flush();

            return getNextInboundEmbedding();
        }
    }

    public void addOutboundEmbedding(O expansion) {
        nextEmbeddingStash.addEmbedding(expansion);
    }

    private static class ODAGLocalCoordinationObject extends WorkerContext.LocalCoordinationObject {
        private ODAGCommunicationStrategy communicationStrategy;
        private WorkerContext workerContext;
        private int numEzipAggregators;
        private int numPartitionsPerWorker;
        private ODAGStash currentUnserializedStash;
        private LinkedHashMap<Pattern, ArrayList<ODAGPartLZ4Wrapper>> receivedParts;
        private Iterator<Map.Entry<Pattern, ArrayList<ODAGPartLZ4Wrapper>>> receivedPartsIterator;
        private ExecutorService mergingPool;

        private AtomicLong totalSizeODAGs;
        private MergingTask[] reusableMergingTasks;
        private ArrayList<Future> futures;

        public ODAGLocalCoordinationObject(WorkerContext workerContext, ODAGCommunicationStrategy communicationStrategy, int numEzipAggregators) {
            this.workerContext = workerContext;
            this.communicationStrategy = communicationStrategy;
            this.numEzipAggregators = numEzipAggregators;
            numPartitionsPerWorker = workerContext.getNumberPartitionsPerWorker();
            mergingPool = Executors.newFixedThreadPool(numPartitionsPerWorker);
            totalSizeODAGs = new AtomicLong(0);

            reusableMergingTasks = new MergingTask[numEzipAggregators];

            for (int i = 0; i < reusableMergingTasks.length; ++i) {
                reusableMergingTasks[i] = new MergingTask();
            }

            futures = new ArrayList<>(reusableMergingTasks.length);
        }

        public ODAGStash getCurrentUnserializedStash() {
            return currentUnserializedStash;
        }

        public AtomicLong getTotalSizeODAGs() {
            return totalSizeODAGs;
        }

        @Override
        public void reset() {
            receivedParts = null;
            receivedPartsIterator = null;
            totalSizeODAGs.set(0);
        }

        @Override
        public void run() {
            if (receivedParts == null) {
                receivedParts = new LinkedHashMap<>();

                Computation computation = communicationStrategy.getExecutionEngine().getComputation();

                for (Writable message : workerContext.getAndClearBroadcastedMessages()) {
                    if (message instanceof ODAGPartLZ4Wrapper) {
                        ODAGPartLZ4Wrapper receivedPart = (ODAGPartLZ4Wrapper) message;

                        Pattern partPattern = receivedPart.getPattern();

                        if (!computation.aggregationFilter(partPattern)) {
                            continue;
                        }

                        ArrayList<ODAGPartLZ4Wrapper> existingPartsForPattern
                                = receivedParts.get(partPattern);

                        if (existingPartsForPattern == null) {
                            existingPartsForPattern = new ArrayList<>(numEzipAggregators);
                            receivedParts.put(receivedPart.getPattern(), existingPartsForPattern);
                        }

                        existingPartsForPattern.add(receivedPart);
                    }
                }
            }

            try {
                if (receivedPartsIterator == null) {
                    receivedPartsIterator = receivedParts.entrySet().iterator();
                }

                long currentEnumerationsInNextMicroStep = 0;

                if (currentUnserializedStash == null) {
                    currentUnserializedStash = new ODAGStash();
                } else {
                    currentUnserializedStash.clear();
                }

                while (receivedPartsIterator.hasNext()) {
                    Map.Entry<Pattern, ArrayList<ODAGPartLZ4Wrapper>> receivedPartsEntry
                            = receivedPartsIterator.next();

                    Pattern pattern = receivedPartsEntry.getKey();
                    ArrayList<ODAGPartLZ4Wrapper> parts = receivedPartsEntry.getValue();

                    ODAG finalEzip = new ODAG(true);

                    finalEzip.setPattern(pattern);

                    futures.clear();

                    int i = 0;
                    for (ODAGPartLZ4Wrapper part : parts) {
                        MergingTask mergingTask = reusableMergingTasks[i];
                        mergingTask.setPart(part);
                        mergingTask.setTargetEzip(finalEzip);
                        futures.add(mergingPool.submit(mergingTask));
                        ++i;
                    }

                    for (Future future : futures) {
                        future.get();
                    }

                    finalEzip.finalizeConstruction(mergingPool, numPartitionsPerWorker);

                    currentUnserializedStash.aggregate(finalEzip);
                    currentEnumerationsInNextMicroStep += finalEzip.getNumberOfEnumerations();

                    receivedPartsIterator.remove();
                }

                if (currentEnumerationsInNextMicroStep == 0) {
                    currentUnserializedStash = null;
                }
            } catch (Throwable e) {
                throw new RuntimeException("Error initializing next stash", e);
            }
        }

        public class MergingTask implements Runnable {
            private ODAG tempEzip;
            private ODAG targetEzip;
            private ODAGPartLZ4Wrapper part;

            public MergingTask() {
                tempEzip = new ODAG(true);
            }

            public void setTargetEzip(ODAG targetEzip) {
                this.targetEzip = targetEzip;
            }

            public ODAGPartLZ4Wrapper getPart() {
                return part;
            }

            public void setPart(ODAGPartLZ4Wrapper part) {
                this.part = part;
            }

            @Override
            public void run() {
                part.ensureDecompressed();
                totalSizeODAGs.addAndGet(part.getByteArrayOutputCache().getPos());
                part.readEzip(tempEzip);
                targetEzip.aggregate(tempEzip);
            }
        }
    }

    private class ODAGLocalCoordinationObjectFactory
            extends WorkerContext.LocalCoordinationObjectFactory {
        private ODAGCommunicationStrategy communicationStrategy;

        public ODAGLocalCoordinationObjectFactory(ODAGCommunicationStrategy communicationStrategy) {
            this.communicationStrategy = communicationStrategy;
        }

        @Override
        public WorkerContext.LocalCoordinationObject create() {
            return new ODAGLocalCoordinationObject(getWorkerContext(), communicationStrategy, numberEzipAggregators);
        }
    }

}
