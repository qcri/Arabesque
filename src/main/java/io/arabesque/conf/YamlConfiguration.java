package io.arabesque.conf;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import io.arabesque.computation.WorkerContext;
import io.arabesque.input.ArabesqueInputFormat;
import io.arabesque.optimization.ConfigBasedOptimizationSetDescriptor;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphConfigurationValidator;
import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

/**
 * YAML based config.
 * <p/>
 * Inspired on Cassandra's configuration management.
 */
public class YamlConfiguration {

    private static final String DEFAULT_CONFIGURATION = "arabesque.default.yaml";
    private static final String DEFAULT_CUSTOM_CONFIGURATION = "arabesque.yaml";
    private static final Logger LOG = Logger.getLogger(YamlConfiguration.class);

    private static final List<String> BASE_CONFIGS = Lists.newArrayList(DEFAULT_CONFIGURATION);


    private static final Options CMDLINE_OPTIONS;
    private static final Map<String, GiraphConfigurationAssignment> VALID_PROPERTIES =
            ImmutableMap.<String, GiraphConfigurationAssignment>builder()
                    // General cluster
                    .put("num_workers", new GiraphConfigurationAssignment() {
                        @Override
                        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
                            Integer numWorkers = Integer.parseInt(propertyValue.toString());
                            giraphConfiguration.setWorkerConfiguration(numWorkers, numWorkers, 100.0f);
                        }
                    })
                    .put("num_compute_threads", new GiraphConfigurationAssignment() {
                        @Override
                        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
                            Integer numComputeThreads = Integer.valueOf(propertyValue.toString());
                            giraphConfiguration.setNumComputeThreads(numComputeThreads);
                        }
                    })
                    .put("num_partitions", new GiraphIntegerConfigurationAssignment(GiraphConstants.USER_PARTITION_COUNT.getKey()))

                    // General Execution
                    .put("verbose", new GiraphDummyConfigurationAssignment()) // Handled on ArabesqueRunner

                    // Computation-specific
                    .put("computation", new GiraphStringConfigurationAssignment(io.arabesque.conf.Configuration.CONF_COMPUTATION_CLASS))
                    .put("master_computation", new GiraphStringConfigurationAssignment(io.arabesque.conf.Configuration.CONF_MASTER_COMPUTATION_CLASS))
                    .put("worker_context", new GiraphClassConfigurationAssignment<WorkerContext>() {
                        @Override
                        public void assign(String propertyKey, Class propertyValue, GiraphConfiguration giraphConfiguration) {
                            giraphConfiguration.setWorkerContextClass(propertyValue);
                        }
                    })
                    .put("pattern", new GiraphStringConfigurationAssignment(io.arabesque.conf.Configuration.CONF_PATTERN_CLASS))
                    .put("communication_strategy", new GiraphConfigurationAssignment() {
                        @Override
                        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
                            String strategy = propertyValue.toString();
                            String strategyFactoryClass = "";

                            switch (strategy) {
                                case "odag":
                                    strategyFactoryClass = "io.arabesque.computation.comm.ODAGCommunicationStrategyFactory";
                                    break;
                                case "embeddings":
                                    strategyFactoryClass = "io.arabesque.computation.comm.CacheCommunicationStrategyFactory";
                                    break;
                                default:
                                    strategyFactoryClass = strategy;
                            }

                            giraphConfiguration.set(io.arabesque.conf.Configuration.CONF_COMM_STRATEGY_FACTORY_CLASS, strategyFactoryClass);
                        }
                    })


                    // Input
                    .put("input_graph_path", new GiraphStringConfigurationAssignment(io.arabesque.conf.Configuration.CONF_MAINGRAPH_PATH))
                    .put("input_graph_local", new GiraphBooleanConfigurationAssignment(io.arabesque.conf.Configuration.CONF_MAINGRAPH_LOCAL))
                    .put("input_graph_edgelabelled", new GiraphBooleanConfigurationAssignment(io.arabesque.conf.Configuration.CONF_MAINGRAPH_EDGE_LABELLED))
                    .put("input_graph_multigraph", new GiraphBooleanConfigurationAssignment(io.arabesque.conf.Configuration.CONF_MAINGRAPH_MULTIGRAPH))

                    // Output
                    .put("output_active", new GiraphBooleanConfigurationAssignment(io.arabesque.conf.Configuration.CONF_OUTPUT_ACTIVE))
                    .put("output_path", new GiraphStringConfigurationAssignment(io.arabesque.conf.Configuration.CONF_OUTPUT_PATH))

                    // Logging
                    .put("log_level", new GiraphStringConfigurationAssignment(GiraphConstants.LOG_LEVEL.getKey()))

                    // Optimizations
                    .put("optimizations", new GiraphStringListConfigurationAssignment(ConfigBasedOptimizationSetDescriptor.CONF_OPTIMIZATION_CLASSES))
                    .build();

    private Set<String> configurations;
    private Map<String, Object> properties;

    static {
        CMDLINE_OPTIONS = new Options();
        Option option = new Option("y", "yaml", true, "YAML configuration file (defaults to arabesque.yaml on classpath)");
        option.setArgs(Option.UNLIMITED_VALUES);
        CMDLINE_OPTIONS.addOption(option);
    }

    public YamlConfiguration() {
        configurations = new LinkedHashSet<>();
        configurations.addAll(BASE_CONFIGS);
        properties = new LinkedHashMap<>();
    }

    public YamlConfiguration(String[] args) {
        this();
        configurations.addAll(findUserProvidedConfigs(args));
    }

    public void load() {
        for (String configPath : configurations) {
            loadConfig(configPath);
        }
    }

    public Object get(String key) {
        return properties.get(key);
    }

    public String getString(String key) {
        return get(key).toString();
    }

    public Integer getInteger(String key) {
        return Integer.parseInt(getString(key));
    }

    public Boolean getBoolean(String key) {
        return Boolean.valueOf(getString(key));
    }

    public void populateGiraphConfiguration(GiraphConfiguration configuration) {
        for (Map.Entry<String, GiraphConfigurationAssignment> property : VALID_PROPERTIES.entrySet()) {
            String propertyKey = property.getKey();

            if (properties.containsKey(propertyKey)) {
                property.getValue().assign(propertyKey, properties.get(propertyKey), configuration);
            }
        }

        addUnrecognizedPropertiesToGiraph(configuration);

        addPrivateConfiguration(configuration);

        validateConfiguration(configuration);
    }

    private void validateConfiguration(GiraphConfiguration configuration) {
        // Perform Giraph's validation code
        GiraphConfigurationValidator<?, ?, ?, ?, ?> gtv =
                new GiraphConfigurationValidator(configuration);
        gtv.validateConfiguration();

        // TODO: Do our own validation
    }

    private void addPrivateConfiguration(GiraphConfiguration configuration) {
        // Set currently fixed classes. TODO: Allow configuration at some point perhaps
        configuration.setComputationClass(io.arabesque.computation.ExecutionEngine.class);
        configuration.setMasterComputeClass(io.arabesque.computation.MasterExecutionEngine.class);
        configuration.setVertexInputFormatClass(ArabesqueInputFormat.class);

        // Calculate partition count based on # of workers and # of threads if no count was provided
        if (GiraphConstants.USER_PARTITION_COUNT.get(configuration) == -1) {
            int numWorkers = configuration.getMinWorkers();
            int numComputeThreads = configuration.getNumComputeThreads();
            GiraphConstants.USER_PARTITION_COUNT.set(configuration, numWorkers * numComputeThreads);
        }

        // Set our default worker context class instead of Giraph's default
        if (GiraphConstants.WORKER_CONTEXT_CLASS.get(configuration).equals(DefaultWorkerContext.class)) {
            configuration.setWorkerContextClass(WorkerContext.class);
        }
    }

    private void addUnrecognizedPropertiesToGiraph(GiraphConfiguration giraphConfiguration) {
        Set<String> unrecognizedKeys = new HashSet<>(properties.keySet());
        unrecognizedKeys.removeAll(VALID_PROPERTIES.keySet());

        for (String unrecognizedKey : unrecognizedKeys) {
            LOG.info("Invalid YAML key, assuming giraph -ca: " + unrecognizedKey);

            Object value = properties.get(unrecognizedKey);
            String valueStr;

            if (!(value instanceof Collection)) {
                valueStr = value.toString();
            } else {
                Collection<Object> collection = (Collection<Object>) value;

                valueStr = StringUtils.join(collection, ',');
            }

            LOG.info("Setting custom argument [" + unrecognizedKey + "] to [" + valueStr + "] in GiraphConfiguration");
            giraphConfiguration.set(unrecognizedKey, valueStr);
        }
    }

    private List<String> findUserProvidedConfigs(String[] args) {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(CMDLINE_OPTIONS, args);

            if (cmd.hasOption("y")) {
                return Arrays.asList(cmd.getOptionValues("y"));
            } else {
                return Collections.singletonList(DEFAULT_CUSTOM_CONFIGURATION);
            }
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse command line parameters", e);
        }
    }

    private void loadConfig(String urlStr) {
        loadConfig(getConfigUrl(urlStr));
    }

    private void loadConfig(URL url) {
        try {
            LOG.info("Loading settings from " + url);

            byte[] configBytes;
            try {
                configBytes = readUrl(url);
            } catch (IOException e) {
                throw new AssertionError(e);
            }

            Yaml yaml = new Yaml();
            Map<String, Object> result = (Map<String, Object>) yaml.load(new ByteArrayInputStream(configBytes));
            properties.putAll(result);
        } catch (YAMLException e) {
            throw new RuntimeException("Invalid yaml", e);
        }
    }

    private URL getConfigUrl(String urlStr) {
        URL url;
        try {
            url = new URL(urlStr);
            url.openStream().close(); // catches well-formed but bogus URLs
        } catch (Exception e) {
            ClassLoader loader = YamlConfiguration.class.getClassLoader();
            url = loader.getResource(urlStr);
            if (url == null) {
                throw new RuntimeException("Unable to find yaml config file location: " + urlStr);
            }
        }

        return url;
    }

    private byte[] readUrl(URL url) throws IOException {
        byte[] configBytes;
        try (InputStream is = url.openStream()) {
            return ByteStreams.toByteArray(is);
        }
    }

    private interface GiraphConfigurationAssignment {
        void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration);
    }

    private static class GiraphDummyConfigurationAssignment implements GiraphConfigurationAssignment {
        @Override
        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            // Empty on purpose
        }
    }

    private static class GiraphIntegerConfigurationAssignment implements GiraphConfigurationAssignment {
        private String giraphConfigurationKey;

        public GiraphIntegerConfigurationAssignment(String giraphConfigurationKey) {
            this.giraphConfigurationKey = giraphConfigurationKey;
        }

        @Override
        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            giraphConfiguration.setInt(giraphConfigurationKey, Integer.parseInt(propertyValue.toString()));
        }
    }

    private static class GiraphBooleanConfigurationAssignment implements GiraphConfigurationAssignment {
        private String giraphConfigurationKey;

        public GiraphBooleanConfigurationAssignment(String giraphConfigurationKey) {
            this.giraphConfigurationKey = giraphConfigurationKey;
        }

        @Override
        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            giraphConfiguration.setBoolean(giraphConfigurationKey, Boolean.valueOf(propertyValue.toString()));
        }
    }

    private static class GiraphStringConfigurationAssignment implements GiraphConfigurationAssignment {
        private String giraphConfigurationKey;

        public GiraphStringConfigurationAssignment(String giraphConfigurationKey) {
            this.giraphConfigurationKey = giraphConfigurationKey;
        }

        @Override
        public void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            giraphConfiguration.set(giraphConfigurationKey, propertyValue.toString());
        }
    }

    private abstract static class GiraphClassConfigurationAssignment<E> implements GiraphConfigurationAssignment {
        @Override
        public final void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            try {
                assign(propertyKey, (Class<E>) Class.forName(propertyValue.toString()), giraphConfiguration);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to set YAML property " + propertyKey + ": " + propertyValue + " class not found", e);
            }
        }

        public abstract void assign(String propertyKey, Class<E> propertyValue, GiraphConfiguration giraphConfiguration);
    }

    private abstract static class GiraphInputHDFSPathConfigurationAssignment implements GiraphConfigurationAssignment {
        @Override
        public final void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            Path path = new Path(propertyValue.toString());

            try {
                if (FileSystem.get(new Configuration()).listStatus(path) == null) {
                    throw new IOException("Null status");
                }

                assign(propertyKey, path, giraphConfiguration);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid hdfs input path (" + propertyKey + "): " + path, e);
            }
        }

        public abstract void assign(String propertyKey, Path path, GiraphConfiguration giraphConfiguration)
                throws IOException;
    }

    private abstract static class GiraphMapConfigurationAssignment implements GiraphConfigurationAssignment {
        @Override
        public final void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            assign(propertyKey, (Map<String, Object>) propertyValue, giraphConfiguration);
        }

        public abstract void assign(String propertyKey, Map<String, Object> properties, GiraphConfiguration giraphConfiguration);
    }

    private static class GiraphStringListConfigurationAssignment implements GiraphConfigurationAssignment {
        private String giraphConfigurationKey;

        public GiraphStringListConfigurationAssignment(String giraphConfigurationKey) {
            this.giraphConfigurationKey = giraphConfigurationKey;
        }

        @Override
        public final void assign(String propertyKey, Object propertyValue, GiraphConfiguration giraphConfiguration) {
            List<Object> values = (List<Object>) propertyValue;
            String[] valuesStrs = new String[values.size()];

            int i = 0;
            for (Object value : values) {
                valuesStrs[i] = value.toString();
                ++i;
            }

            giraphConfiguration.setStrings(giraphConfigurationKey, valuesStrs);
        }
    }
}
