package io.arabesque.conf;

import io.arabesque.aggregation.AggregationStorageMetadata;
import io.arabesque.aggregation.EndAggregationFunction;
import io.arabesque.aggregation.reductions.ReductionFunction;
import io.arabesque.computation.Computation;
import io.arabesque.computation.ExecutionEngine;
import io.arabesque.computation.MasterComputation;
import io.arabesque.computation.WorkerContext;
import io.arabesque.computation.comm.CommunicationStrategy;
import io.arabesque.computation.comm.CommunicationStrategyFactory;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.BasicMainGraph;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Configuration<O extends Embedding> {
    public static final int KB = 1024;
    public static final int MB = 1024 * KB;
    public static final int GB = 1024 * MB;

    public static final int SECONDS = 1000;
    public static final int MINUTES = 60 * SECONDS;
    public static final int HOURS = 60 * MINUTES;

    public static final int K = 1000;
    public static final int M = 1000 * K;
    public static final int B = 1000 * M;

    public static final String CONF_MAINGRAPH_PATH = "arabesque.graph.location";
    public static final String CONF_MAINGRAPH_PATH_DEFAULT = "main.graph";
    public static final String CONF_MAINGRAPH_LOCAL = "arabesque.graph.local";
    public static final boolean CONF_MAINGRAPH_LOCAL_DEFAULT = false;

    private static final String CONF_COMPRESSED_CACHES = "arabesque.caches.compress";
    private static final boolean CONF_COMPRESSED_CACHES_DEFAULT = false;
    private static final String CONF_CACHE_THRESHOLD_SIZE = "arabesque.cache.threshold";
    private static final int CONF_CACHE_THRESHOLD_SIZE_DEFAULT = 1 * MB;

    public static final String CONF_OUTPUT_ACTIVE = "arabesque.output.active";
    public static final boolean CONF_OUTPUT_ACTIVE_DEFAULT = true;

    public static final String INFO_PERIOD = "arabesque.info.period";
    public static final long INFO_PERIOD_DEFAULT = 60000;

    public static final String CONF_COMPUTATION_CLASS = "arabesque.computation.class";
    public static final String CONF_COMPUTATION_CLASS_DEFAULT = "";

    public static final String CONF_MASTER_COMPUTATION_CLASS = "arabesque.master_computation.class";
    public static final String CONF_MASTER_COMPUTATION_CLASS_DEFAULT = "io.arabesque.computation.MasterComputation";

    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS = "arabesque.comm.factory.class";
    public static final String CONF_COMM_STRATEGY_FACTORY_CLASS_DEFAULT = "io.arabesque.computation.comm.ODAGCommunicationStrategyFactory";

    public static final String CONF_PATTERN_CLASS = "arabesque.pattern.class";
    public static final String CONF_PATTERN_CLASS_DEFAULT = "io.arabesque.pattern.JBlissPattern";

    private static final String CONF_EZIP_AGGREGATORS = "arabesque.odag.aggregators";
    private static final int CONF_EZIP_AGGREGATORS_DEFAULT = -1;

    private static final String CONF_2LEVELAGG_ENABLED = "arabesque.2levelagg.enabled";
    private static final boolean CONF_2LEVELAGG_ENABLED_DEFAULT = true;
    private static final String CONF_FORCE_GC = "arabesque.forcegc";
    private static final boolean CONF_FORCE_GC_DEFAULT = false;

    public static final String CONF_OUTPUT_PATH = "arabesque.output.path";
    public static final String CONF_OUTPUT_PATH_DEFAULT = "Output";

    public static final String CONF_DEFAULT_AGGREGATOR_SPLITS = "arabesque.aggregators.default_splits";
    public static final int CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT = 1;

    protected static Configuration instance = null;
    private ImmutableClassesGiraphConfiguration giraphConfiguration;

    private boolean useCompressedCaches;
    private int cacheThresholdSize;
    private long infoPeriod;
    private int odagNumAggregators;
    private boolean is2LevelAggregationEnabled;
    private boolean forceGC;
    private CommunicationStrategyFactory communicationStrategyFactory;
    private Class<? extends Pattern> patternClass;
    private Class<? extends Computation> computationClass;
    private Class<? extends MasterComputation> masterComputationClass;
    private String outputPath;
    private int defaultAggregatorSplits;

    private Map<String, AggregationStorageMetadata> aggregationsMetadata;

    public static <C extends Configuration> C get() {
        if (instance == null) {
            throw new RuntimeException("Oh-oh, Null configuration");
        }

        return (C) instance;
    }

    public static void setIfUnset(Configuration configuration) {
        if (instance == null) {
            set(configuration);
        }
    }

    public static void set(Configuration configuration) {
        instance = configuration;
    }

    public Configuration(ImmutableClassesGiraphConfiguration giraphConfiguration) {
        this.giraphConfiguration = giraphConfiguration;

        useCompressedCaches = getBoolean(CONF_COMPRESSED_CACHES, CONF_COMPRESSED_CACHES_DEFAULT);
        cacheThresholdSize = getInteger(CONF_CACHE_THRESHOLD_SIZE, CONF_CACHE_THRESHOLD_SIZE_DEFAULT);
        infoPeriod = getLong(INFO_PERIOD, INFO_PERIOD_DEFAULT);
        Class<? extends CommunicationStrategyFactory> communicationStrategyFactoryClass =
                (Class<? extends CommunicationStrategyFactory>) getClass(CONF_COMM_STRATEGY_FACTORY_CLASS, CONF_COMM_STRATEGY_FACTORY_CLASS_DEFAULT);
        communicationStrategyFactory = ReflectionUtils.newInstance(communicationStrategyFactoryClass);
        odagNumAggregators = getInteger(CONF_EZIP_AGGREGATORS, CONF_EZIP_AGGREGATORS_DEFAULT);
        is2LevelAggregationEnabled = getBoolean(CONF_2LEVELAGG_ENABLED, CONF_2LEVELAGG_ENABLED_DEFAULT);
        forceGC = getBoolean(CONF_FORCE_GC, CONF_FORCE_GC_DEFAULT);
        patternClass = (Class<? extends Pattern>) getClass(CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT);
        computationClass = (Class<? extends Computation>) getClass(CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT);
        masterComputationClass = (Class<? extends MasterComputation>) getClass(CONF_MASTER_COMPUTATION_CLASS, CONF_MASTER_COMPUTATION_CLASS_DEFAULT);

        aggregationsMetadata = new HashMap<>();

        outputPath = getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT);

        defaultAggregatorSplits = getInteger(CONF_DEFAULT_AGGREGATOR_SPLITS, CONF_DEFAULT_AGGREGATOR_SPLITS_DEFAULT);
    }

    public ImmutableClassesGiraphConfiguration getUnderlyingConfiguration() {
        return giraphConfiguration;
    }

    public String getString(String key, String defaultValue) {
        return giraphConfiguration.get(key, defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        return giraphConfiguration.getBoolean(key, defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        return giraphConfiguration.getInt(key, defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        return giraphConfiguration.getLong(key, defaultValue);
    }

    public Float getFloat(String key, Float defaultValue) {
        return giraphConfiguration.getFloat(key, defaultValue);
    }

    public Class<?> getClass(String key, String defaultValue) {
        try {
            return Class.forName(getString(key, defaultValue));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends Pattern> getPatternClass() {
        return patternClass;
    }

    public Pattern createPattern() {
        return ReflectionUtils.newInstance(getPatternClass());
    }

    public boolean isUseCompressedCaches() {
        return useCompressedCaches;
    }

    public int getCacheThresholdSize() {
        return cacheThresholdSize;
    }

    public String getMainGraphPath() {
        return getString(CONF_MAINGRAPH_PATH, CONF_MAINGRAPH_PATH_DEFAULT);
    }

    public long getInfoPeriod() {
        return infoPeriod;
    }

    public MainGraph createGraph() {
        boolean useLocalGraph = getBoolean(CONF_MAINGRAPH_LOCAL, CONF_MAINGRAPH_LOCAL_DEFAULT);

        try {
            if (useLocalGraph) {
                return new BasicMainGraph(Paths.get(getMainGraphPath()));
            } else {
                return new BasicMainGraph(new Path(getMainGraphPath()));
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not load main graph", e);
        }
    }

    public boolean isOutputActive() {
        return getBoolean(CONF_OUTPUT_ACTIVE, CONF_OUTPUT_ACTIVE_DEFAULT);
    }

    public int getODAGNumAggregators() {
        return odagNumAggregators;
    }

    public int getMaxEnumerationsPerMicroStep() {
        return 10000000;
    }

    public boolean is2LevelAggregationEnabled() {
        return is2LevelAggregationEnabled;
    }

    public boolean isForceGC() {
        return forceGC;
    }

    public <M extends Writable> M createCommunicationStrategyMessage() {
        return communicationStrategyFactory.createMessage();
    }

    public CommunicationStrategy<O> createCommunicationStrategy(Configuration<O> configuration,
            ExecutionEngine<O> executionEngine, WorkerContext workerContext) {
        CommunicationStrategy<O> commStrategy = communicationStrategyFactory.createCommunicationStrategy();

        commStrategy.setConfiguration(configuration);
        commStrategy.setExecutionEngine(executionEngine);
        commStrategy.setWorkerContext(workerContext);

        return commStrategy;
    }

    public Set<String> getRegisteredAggregations() {
        return Collections.unmodifiableSet(aggregationsMetadata.keySet());
    }

    public Map<String, AggregationStorageMetadata> getAggregationsMetadata() {
        return Collections.unmodifiableMap(aggregationsMetadata);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction, EndAggregationFunction<K, V> endAggregationFunction, int numSplits) {
        if (aggregationsMetadata.containsKey(name)) {
            return;
        }

        AggregationStorageMetadata<K, V> aggregationMetadata =
                new AggregationStorageMetadata<>(keyClass, valueClass, persistent, reductionFunction, endAggregationFunction, numSplits);

        aggregationsMetadata.put(name, aggregationMetadata);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction) {
    	registerAggregation(name, keyClass, valueClass, persistent, reductionFunction, null, defaultAggregatorSplits);
    }

    public <K extends Writable, V extends Writable>
    void registerAggregation(String name, Class<K> keyClass, Class<V> valueClass, boolean persistent, ReductionFunction<V> reductionFunction, EndAggregationFunction<K, V> endAggregationFunction) {
    	registerAggregation(name, keyClass, valueClass, persistent, reductionFunction, endAggregationFunction, defaultAggregatorSplits);
    }

    public <K extends Writable, V extends Writable> AggregationStorageMetadata<K, V> getAggregationMetadata(String name) {
        return (AggregationStorageMetadata<K, V>) aggregationsMetadata.get(name);
    }

    public String getAggregationSplitName(String name, int splitId) {
        return name + "_" + splitId;
    }

    public <O extends Embedding> Computation<O> createComputation() {
        return ReflectionUtils.newInstance(computationClass);
    }

    public String getOutputPath() {
        return outputPath;
    }

    public MasterComputation createMasterComputation() {
        return ReflectionUtils.newInstance(masterComputationClass);
    }
}

