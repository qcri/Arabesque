package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.AggregationStorageFactory;
import io.arabesque.aggregation.AggregationStorageMetadata;
import io.arabesque.aggregation.AggregationStorageWrapper;
import io.arabesque.conf.Configuration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerContext extends org.apache.giraph.worker.WorkerContext {
    private static final Logger LOG = Logger.getLogger(WorkerContext.class);
    private static Path OUTPUT_PATH = null;

    private CentralizedServiceWorker<?, ?, ?> serviceWorker;
    private int numberPartitions;
    private Map<String, String> data;
    private int numPhases;

    // Map<Depth, Num Embeddings processed>
    private ConcurrentMap<Integer, AtomicLong> depthProcessedInfo;
    private int maxProcessedDepthReached;
    private Timer infoTimer;
    private OutputStreamWriter outputStream;
    private OutputStreamWriter aggregationOutputStream;

    private CyclicBarrier barrier;
    private LocalCoordinationObject localCoordinationObject;

    private AggregationStorageFactory aggregationStorageFactory;
    private Map<String, AggregationStorage> outboundAggregationStorages;
    private Map<String, AggregationStorage> inboundAggregationStorages;

    public WorkerContext() {
        data = new ConcurrentHashMap<>();
        numberPartitions = 0;
        maxProcessedDepthReached = 0;
        this.aggregationStorageFactory = new AggregationStorageFactory();
        this.outboundAggregationStorages = new ConcurrentHashMap<>();
        this.inboundAggregationStorages = new ConcurrentHashMap<>();
    }

    public void reportProcessedInfo(int depth, long numberEmbeddings) {
        depthProcessedInfo.putIfAbsent(depth, new AtomicLong(0));
        depthProcessedInfo.get(depth).addAndGet(numberEmbeddings);
        maxProcessedDepthReached = Math.max(maxProcessedDepthReached, depth);
    }

    public int getNumberPartitionsPerWorker() {
        return getNumberPartitions() / getWorkerCount();
    }

    public List<Writable> getAndClearBroadcastedMessages() {
        return getAndClearMessagesFromOtherWorkers();
    }


    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<WritableComparable, Writable, Writable> conf) {
        super.setConf(conf);
        Configuration.setIfUnset(createConfiguration(conf));
        Configuration.get().initialize();

        if (Configuration.get().isForceGC()) {
            System.gc();
        }

        depthProcessedInfo = new ConcurrentHashMap<>(16, 0.75f, getConf().getNumComputeThreads());

        OUTPUT_PATH = new Path(Configuration.get().getOutputPath());
    }

    @Override
    public void setupSuperstep(CentralizedServiceWorker<?, ?, ?> serviceWorker) {
        super.setupSuperstep(serviceWorker);
        this.serviceWorker = serviceWorker;
    }

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {
        infoTimer = new Timer(true);
        infoTimer.schedule(this.new PeriodicInfoTask(), Configuration.get().getInfoPeriod(),
                Configuration.get().getInfoPeriod());

        if (Configuration.get().isOutputActive()) {
            try {
                FileSystem fs = FileSystem.get(getConf());
                outputStream = new OutputStreamWriter(
                        fs.create(
                                new Path(OUTPUT_PATH,
                                        Integer.toString(getConf().getTaskPartition()))));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        numPhases = Configuration.get().createCommunicationStrategy(null, null, null).getNumPhases();
    }

    @Override
    public void postApplication() {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (getMyWorkerIndex() == 0) {
        	Map<String, AggregationStorageMetadata> aggregationMap = Configuration.get().getAggregationsMetadata();
        	for (Map.Entry<String, AggregationStorageMetadata> aggregation : aggregationMap.entrySet()){
        		if (aggregation.getValue().isPersistent()) {
	                System.out.println("Output aggregation: ");
	                AggregationStorage aggregationStorage = getAggregatedValue(aggregation.getKey());
	                System.out.println(aggregationStorage.toOutputString());
	                try {
	                    FileSystem fs = FileSystem.get(getConf());
	                    aggregationOutputStream = new OutputStreamWriter(
	                            fs.create(
	                                    new Path(OUTPUT_PATH,
	                                            "aggregation-" + aggregationStorage.getName())));
	                    aggregationOutputStream.write(aggregationStorage.toOutputString() + '\n');
	                    aggregationOutputStream.close();
	                } catch (IOException e) {
	                    throw new RuntimeException(e);
	                }
	            }
        	}
        }
    }

    public void output(String outputString) {
        if (outputStream == null) {
            return;
        }

        try {
            outputStream.write(outputString + '\n');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preSuperstep() {
        numberPartitions = 0;
        for (PartitionOwner partitionOwner : serviceWorker.getPartitionOwners()) {
            ++numberPartitions;
        }

        resetLocalCoordination();

        inboundAggregationStorages.clear();
        outboundAggregationStorages.clear();
    }

    @Override
    public void postSuperstep() {
        for (String name : outboundAggregationStorages.keySet()) {
            splitAggregationStoragesAndSend(name);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeInt(data.size());

        for (Map.Entry<String, String> dataEntry : data.entrySet()) {
            dataOutput.writeInt(dataEntry.getKey().length());
            dataOutput.writeChars(dataEntry.getKey());

            dataOutput.writeInt(dataEntry.getValue().length());
            dataOutput.writeChars(dataEntry.getValue());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        data.clear();

        StringBuilder strBuilder = new StringBuilder(256);

        int numberOfDataEntries = dataInput.readInt();

        for (int i = 0; i < numberOfDataEntries; ++i) {

            int entryKeySize = dataInput.readInt();

            strBuilder.setLength(0);
            for (int j = 0; j < entryKeySize; ++j) {
                strBuilder.append(dataInput.readChar());
            }

            String key = strBuilder.toString();

            int entryValueSize = dataInput.readInt();

            strBuilder.setLength(0);
            for (int j = 0; j < entryValueSize; ++j) {
                strBuilder.append(dataInput.readChar());
            }

            String value = strBuilder.toString();

            data.put(key, value);
        }
    }

    public int getNumberPartitions() {
        return numberPartitions;
    }

    public String getStringData(String key) {
        return data.get(key);
    }

    public Integer getIntegerData(String key) {
        String valueString = data.get(key);

        if (valueString != null) {
            return Integer.parseInt(valueString);
        } else {
            return null;
        }
    }

    public long getLongData(String key) {
        String valueString = data.get(key);

        if (valueString != null) {
            return Long.parseLong(valueString);
        } else {
            return -1;
        }
    }

    public void setData(String key, Object value) {
        data.put(key, value.toString());
    }

    protected Configuration createConfiguration(ImmutableClassesGiraphConfiguration giraphConf) {
        return new Configuration(giraphConf);
    }

    @Override
    public String toString() {
        return "WorkerContext{" +
                "numberPartitions=" + numberPartitions +
                ", data=" + data +
                "} " + super.toString();
    }

    public void broadcast(Writable writable) throws IOException {
        int numWorkers = getWorkerCount();

        for (int j = 0; j < numWorkers; ++j) {
            sendMessageToWorker(writable, j);
        }
    }

    public synchronized void setupLocalCoordination(LocalCoordinationObjectFactory coordinationObjectFactory) {
        if (barrier != null) {
            return;
        }

        int numPartitionsPerWorker = getNumberPartitionsPerWorker();

        // Sanity check, if # partitions > # threads, barrier will never exit
        int numComputeThreads = getConf().getNumComputeThreads();

        if (numPartitionsPerWorker > numComputeThreads) {
            throw new RuntimeException("Cannot use local coordination when #partitions/worker (" +
                    numPartitionsPerWorker + ") > #computeThreads (" + numComputeThreads + ")");
        }

        localCoordinationObject = coordinationObjectFactory.create();
        barrier = new CyclicBarrier(numPartitionsPerWorker, localCoordinationObject);
    }

    public synchronized void resetLocalCoordination() {
        barrier = null;
        localCoordinationObject = null;
        /*if (localCoordinationObject != null) {
            localCoordinationObject.reset();
        }*/
    }

    public LocalCoordinationObject accessLocalCoordinationObject() {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }

        return localCoordinationObject;
    }

    public void addAggregationStorage(String name, AggregationStorage aggregationStorage) {
        AggregationStorage finalAggregationStorage = getOutboundAggregationStorage(name);

        finalAggregationStorage.finalLocalAggregate(aggregationStorage);
    }

    private <K extends Writable, V extends Writable> AggregationStorage<K, V> getInboundAggregationStorage(String name) {
        AggregationStorage<K, V> aggregationStorage = inboundAggregationStorages.get(name);

        if (aggregationStorage == null) {
            synchronized (this) {
                aggregationStorage = inboundAggregationStorages.get(name);

                if (aggregationStorage == null) {
                    aggregationStorage = constructStorage(name);

                    inboundAggregationStorages.put(name, aggregationStorage);
                }
            }
        }

        return aggregationStorage;
    }

    private <K extends Writable, V extends Writable> AggregationStorage<K, V> getOutboundAggregationStorage(String name) {
        AggregationStorage<K, V> aggregationStorage = outboundAggregationStorages.get(name);

        if (aggregationStorage == null) {
            synchronized (this) {
                aggregationStorage = outboundAggregationStorages.get(name);

                if (aggregationStorage == null) {
                    aggregationStorage = aggregationStorageFactory.createAggregationStorage(name);
                    outboundAggregationStorages.put(name, aggregationStorage);
                }
            }
        }

        return aggregationStorage;
    }

    public static abstract class LocalCoordinationObject implements Runnable {
        public void reset() {
            // Empty by design
        }
    }

    public static abstract class LocalCoordinationObjectFactory {
        public abstract LocalCoordinationObject create();
    }

    protected class PeriodicInfoTask extends TimerTask {
        @Override
        public void run() {
            Runtime runtime = Runtime.getRuntime();

            double unit = Configuration.GB;

            System.out.println("##### Heap utilization statistics [GB] #####");

            //Print used memory
            System.out.println("Used Memory:"
                    + (runtime.totalMemory() - runtime.freeMemory()) / unit);

            //Print free memory
            System.out.println("Free Memory:"
                    + runtime.freeMemory() / unit);

            //Print total available memory
            System.out.println("Total Memory:" + runtime.totalMemory() / unit);

            //Print Maximum available memory
            System.out.println("Max Memory:" + runtime.maxMemory() / unit);

            long totalEmbeddingsProcessed = 0;

            for (Map.Entry<Integer, AtomicLong> depthProcessedEntry : depthProcessedInfo.entrySet()) {
                System.out.println("Depth: " + depthProcessedEntry.getKey() + " | Processed " + depthProcessedEntry.getValue() + " (" + (depthProcessedEntry.getValue().get() * getWorkerCount()) + ")");
            }

            //Total embeddings processed
            System.out.println("Embeddings processed: " + totalEmbeddingsProcessed);
        }
    }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        if (metadata == null) {
            return super.getAggregatedValue(name);
        } else {
            return (A) getInboundAggregationStorage(name);
        }
    }

    private AggregationStorage constructStorage(String name) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        int numSplits = metadata.getNumSplits();

        AggregationStorageWrapper storageWrapper = new AggregationStorageWrapper();

        for (int i = 0; i < numSplits; ++i) {
            AggregationStorageWrapper storageWrapperSplit = super.getAggregatedValue(Configuration.get().getAggregationSplitName(name, i));
            storageWrapper.aggregate(storageWrapperSplit);
        }


        AggregationStorage storage = storageWrapper.getAggregationStorage();

        if (storage != null) {
            return storageWrapper.getAggregationStorage();
        } else {
            return aggregationStorageFactory.createAggregationStorage(name, metadata);
        }
    }

    private void splitAggregationStoragesAndSend(String name) {
        Configuration conf = Configuration.get();
        AggregationStorage aggregationStorage = outboundAggregationStorages.get(name);

        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        int numSplits = metadata.getNumSplits();

        if (numSplits == 1) {
            AggregationStorageWrapper aggWrapper = new AggregationStorageWrapper(aggregationStorage);
            aggregate(conf.getAggregationSplitName(name, 0), aggWrapper);
        } else {
            Collection<Writable> keysToTransfer = new ArrayList<>(aggregationStorage.getNumberMappings());

            for (int i = 0; i < numSplits; ++i) {
                AggregationStorage aggStorageSplit = aggregationStorageFactory.createAggregationStorage(name);
                AggregationStorageWrapper aggStorageSplitWrapper = new AggregationStorageWrapper(aggStorageSplit);

                keysToTransfer.clear();

                for (Object key : aggregationStorage.getKeys()) {
                    int owner = key.hashCode() % numSplits;

                    if (owner < 0) {
                        owner += numSplits;
                    }

                    if (owner != i) {
                        continue;
                    }

                    keysToTransfer.add((Writable) key);
                }

                for (Writable key : keysToTransfer) {
                    aggStorageSplit.transferKeyFrom(key, aggregationStorage);
                }

                aggregate(conf.getAggregationSplitName(name, i), aggStorageSplitWrapper);
            }
        }
    }
}
