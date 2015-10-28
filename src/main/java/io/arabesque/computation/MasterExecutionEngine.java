package io.arabesque.computation;

import io.arabesque.aggregation.*;
import io.arabesque.computation.comm.CommunicationStrategy;
import io.arabesque.conf.Configuration;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class MasterExecutionEngine extends MasterCompute {
    public static final String AGG_EMBEDDINGS_PROCESSED = "embeddings_processed";
    public static final String AGG_PROCESSED_SIZE_CACHE = "processed_size_cache";
    public static final String AGG_PROCESSED_SIZE_ODAG = "processed_size_odag";
    public static final String AGG_EMBEDDINGS_GENERATED = "embeddings_generated";
    public static final String AGG_EMBEDDINGS_OUTPUT = "embeddings_output";
    public static final String AGG_CHILDREN_EVALUATED = "children_evaluated";

    private static final Logger LOG = Logger.getLogger(MasterExecutionEngine.class);

    private int numPhases;
    private int currentPhase;
    private ArrayList<String> registeredAggregatorNames;
    private Map<String, Writable> savedAggregatorValues;

    private Map<String, AggregationStorage> inboundAggregationStorages;

    private AggregationStorageFactory aggregationStorageFactory;

    private MasterComputation masterComputation;

    public MasterExecutionEngine() {
        inboundAggregationStorages = new HashMap<>();
        aggregationStorageFactory = new AggregationStorageFactory();
    }

    @Override
    public <A extends Writable> boolean registerAggregator(String name,
            Class<? extends Aggregator<A>> aggregatorClass)
            throws InstantiationException, IllegalAccessException {
        boolean result = super.registerAggregator(name, aggregatorClass);

        if (result) {
            registeredAggregatorNames.add(name);
        }

        return result;
    }

    @Override
    public long getSuperstep() {
        return super.getSuperstep() / numPhases;
    }

    @Override
    public final void compute() {
        inboundAggregationStorages.clear();

        if (numPhases == 1) {
            internalCompute();
        } else {
            currentPhase = (int) (super.getSuperstep() % numPhases);

            LOG.info("MASTER: Real SS" + super.getSuperstep());
            LOG.info("MASTER: Fake SS" + getSuperstep());
            LOG.info("MASTER: Phase" + currentPhase);

            if (currentPhase == 1) {
                internalCompute();

                for (String registeredAggregatorName : registeredAggregatorNames) {
                    Writable value = super.getAggregatedValue(registeredAggregatorName);
                    savedAggregatorValues.put(registeredAggregatorName, value);
                    setAggregatedValue(registeredAggregatorName, null);
                }
            } else if (currentPhase == 0) {
                for (Map.Entry<String, Writable> savedAggregatorValuesEntry : savedAggregatorValues.entrySet()) {
                    setAggregatedValue(savedAggregatorValuesEntry.getKey(), savedAggregatorValuesEntry.getValue());
                }

                savedAggregatorValues.clear();
            }
        }
    }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        if (metadata == null) {
            return super.getAggregatedValue(name);
        } else {
            AggregationStorage aggregationStorage = inboundAggregationStorages.get(name);

            if (aggregationStorage == null) {
                int numSplits = metadata.getNumSplits();

                AggregationStorageWrapper storageWrapper = new AggregationStorageWrapper();

                for (int i = 0; i < numSplits; ++i) {
                    AggregationStorageWrapper storageWrapperSplit
                            = getAggregatedValue(Configuration.get().getAggregationSplitName(name, i));
                    storageWrapper.aggregate(storageWrapperSplit);
                }

                aggregationStorage = storageWrapper.getAggregationStorage();

                if (aggregationStorage == null) {
                    aggregationStorage = aggregationStorageFactory.createAggregationStorage(name, metadata);
                }

                inboundAggregationStorages.put(name, aggregationStorage);
            }

            return (A) aggregationStorage;
        }
    }

    @Override
    public <A extends Writable> void setAggregatedValue(String name, A value) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        if (metadata == null) {
            super.setAggregatedValue(name, value);
        } else {
            if (!(value instanceof AggregationStorage)) {
                throw new RuntimeException("Value of an Arabesque aggregation should be a subclass of AggregationStorage");
            }

            AggregationStorage aggregationStorage = (AggregationStorage) value;

            splitAggregationStoragesAndSend(name, metadata, aggregationStorage);
        }
    }

    private void splitAggregationStoragesAndSend(String name, AggregationStorageMetadata metadata, AggregationStorage aggregationStorage) {
        Configuration conf = Configuration.get();

        int numSplits = metadata.getNumSplits();

        if (numSplits == 1) {
            AggregationStorageWrapper aggWrapper = new AggregationStorageWrapper(aggregationStorage);
            super.setAggregatedValue(conf.getAggregationSplitName(name, 0), aggWrapper);
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

                super.setAggregatedValue(conf.getAggregationSplitName(name, i), aggStorageSplitWrapper);
            }
        }
    }

    protected void internalCompute() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println("End of SS " + getSuperstep() + ":" + dateFormat.format(date));
        System.out.println("Embeddings processed: " + getAggregatedValue(AGG_EMBEDDINGS_PROCESSED));

        LongWritable processedSizeODAGWritable = getAggregatedValue(AGG_PROCESSED_SIZE_ODAG);
        long processedSizeODAG = processedSizeODAGWritable.get();

        if (processedSizeODAG > 0) {
            System.out.println("Embeddings processed size (odag): " + processedSizeODAG);
        }

        LongWritable processedSizeCacheWritable = getAggregatedValue(AGG_PROCESSED_SIZE_CACHE);
        long processedSizeCache = processedSizeCacheWritable.get();

        if (processedSizeCache > 0) {
            System.out.println("Embeddings processed size (cache): " + processedSizeCache);
        }

        System.out.println("Embeddings generated: " + getAggregatedValue(AGG_EMBEDDINGS_GENERATED));
        System.out.println("Children evaluated: " + getAggregatedValue(AGG_CHILDREN_EVALUATED));
        System.out.println("Embeddings output: " + getAggregatedValue(AGG_EMBEDDINGS_OUTPUT));

        LongWritable numEmbeddingsProcessed = getAggregatedValue(AGG_EMBEDDINGS_PROCESSED);
        LongWritable numEmbeddingsGenerated = getAggregatedValue(AGG_EMBEDDINGS_GENERATED);

        // If we processed and generated no embeddings on the last superstep, execution has finished
        if (getSuperstep() > 0 && numEmbeddingsProcessed.get() == 0 && numEmbeddingsGenerated.get() == 0) {
            haltComputation();
        }

        masterComputation.compute();
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        // If we run with separate master, we need to initialize the worker context
        // ourselves because a worker context is never created (and the worker context
        // is the one that initializes our configuration according to the user settings).
        WorkerContext workerContext = (WorkerContext) getConf().createWorkerContext();
        workerContext.setConf(getConf());

        Configuration conf = Configuration.get();

        CommunicationStrategy communicationStrategy = conf.createCommunicationStrategy(conf, null, workerContext);

        numPhases = communicationStrategy.getNumPhases();

        registeredAggregatorNames = new ArrayList<>();
        savedAggregatorValues = new HashMap<>();

        registerAggregator(AGG_EMBEDDINGS_GENERATED, LongSumAggregator.class);
        registerAggregator(AGG_EMBEDDINGS_PROCESSED, LongSumAggregator.class);
        registerAggregator(AGG_PROCESSED_SIZE_ODAG, LongMaxAggregator.class);
        registerAggregator(AGG_PROCESSED_SIZE_CACHE, LongSumAggregator.class);
        registerAggregator(AGG_CHILDREN_EVALUATED, LongSumAggregator.class);
        registerAggregator(AGG_EMBEDDINGS_OUTPUT, LongSumAggregator.class);

        Computation<?> computation = conf.createComputation();

        computation.initAggregations();

        Map<String, AggregationStorageMetadata> registeredAggregationStorages =
                conf.getAggregationsMetadata();

        LOG.info("Registered aggregation storages: " + registeredAggregationStorages);

        for (Map.Entry<String, AggregationStorageMetadata> entry : registeredAggregationStorages.entrySet()) {
            String aggName = entry.getKey();
            AggregationStorageMetadata aggStorageMetadata = entry.getValue();

            registerAggregationStore(aggName, aggStorageMetadata);
        }

        masterComputation = conf.createMasterComputation();
        masterComputation.setUnderlyingExecutionEngine(this);
        masterComputation.init();
    }

    private void registerAggregationStore(String aggName, AggregationStorageMetadata aggStorageMetadata) throws IllegalAccessException, InstantiationException {
        boolean isPersistent = aggStorageMetadata.isPersistent();
        int numSplits = aggStorageMetadata.getNumSplits();

        for (int i = 0; i < numSplits; ++i) {
            String splitName = Configuration.get().getAggregationSplitName(aggName, i);
            if (isPersistent) {
                registerPersistentAggregator(splitName, AggregationStorageAggregator.class);
            } else {
                registerAggregator(splitName, AggregationStorageAggregator.class);
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Empty by design (no state to store)
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // Empty by design (no state to read)
    }
}
