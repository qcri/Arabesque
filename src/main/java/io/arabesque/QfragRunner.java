package io.arabesque;

import io.arabesque.conf.SparkConfiguration;
import io.arabesque.conf.YamlConfiguration;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.search.steps.EmbeddingEnumeration;
import io.arabesque.search.steps.QueryGraph;
import io.arabesque.search.steps.TreeBuilding;
import io.arabesque.search.trees.SearchDataTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ehussein on 11/16/17.
 */
public class QfragRunner implements Tool {
    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(QfragRunner.class);
    /**
     * Writable io.arabesque.conf
     */
    private Configuration conf;

    private int numPartitions = 0;
    private SparkConfiguration config = null;
    private JavaSparkContext sc = null;

    Broadcast<SparkConfiguration> configBC;
    Broadcast<QueryGraph> queryGraphBC;

    private String inputGraphPath;
    private String queryGraphPath;

    // accumulators
    private Map<String, CollectionAccumulator<Long>> aggAccums;
    public final static String TREE_BUILDING_INIT_START_TIME = "TREE_BUILDING_INIT_START_TIME";
    public final static String TREE_BUILDING_INIT_FINISH_TIME = "TREE_BUILDING_INIT_FINISH_TIME";
    public final static String TREE_BUILDING_COMPUTATION_START_TIME = "TREE_BUILDING_COMPUTATION_START_TIME";
    public final static String TREE_BUILDING_COMPUTATION_FINISH_TIME = "TREE_BUILDING_COMPUTATION_FINISH_TIME";

    public final static String EMBEDDING_ENUMERATION_INIT_START_TIME = "EMBEDDING_ENUMERATION_INIT_START_TIME";
    public final static String EMBEDDING_ENUMERATION_INIT_FINISH_TIME = "EMBEDDING_ENUMERATION_INIT_FINISH_TIME";
    public final static String EMBEDDING_ENUMERATION_COMPUTATION_START_TIME = "EMBEDDING_ENUMERATION_COMPUTATION_START_TIME";
    public final static String EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME = "EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME";

    long dataGraphBuildingTime;
    long queryGraphBuildingTime;
    long treeBuildingComputationTime;
    long embeddingEnumerationComputationTime;
    long totalComputationTime;

    long startComputeTime;
    long endComputeTime;
    long computeTime;
    long shuffleTime;

    private void init(String[] args) {
        YamlConfiguration yamlConfig = new YamlConfiguration(args);
        yamlConfig.load();

        config = new SparkConfiguration(JavaConversions.mapAsScalaMap(yamlConfig.getProperties()));
        sc = new JavaSparkContext(config.sparkConf());

        String log_level = config.getLogLevel();
        LOG.fatal("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));
        sc.setLogLevel(log_level.toUpperCase());
        config.setIfUnset ("num_partitions", sc.defaultParallelism());

        config.setHadoopConfig (sc.hadoopConfiguration());
        numPartitions = config.numPartitions();
        inputGraphPath = config.getString(config.SEARCH_MAINGRAPH_PATH,config.SEARCH_MAINGRAPH_PATH_DEFAULT);
        queryGraphPath = config.getString(config.SEARCH_QUERY_GRAPH_PATH,config.SEARCH_QUERY_GRAPH_PATH_DEFAULT);

        dataGraphBuildingTime = System.currentTimeMillis();

        UnsafeCSRGraphSearch dataGraph = null;
        try {
            if(inputGraphPath == null)
                throw new RuntimeException("Main input graph was not set in the config file");
            dataGraph = new UnsafeCSRGraphSearch(new org.apache.hadoop.fs.Path(inputGraphPath));
        } catch (IOException e) {
            System.out.println("Error reading the data graph");
            System.out.println(e.toString());
        }

        dataGraphBuildingTime = System.currentTimeMillis() - dataGraphBuildingTime;

        config.setSearchMainGraph(dataGraph);

        queryGraphBuildingTime = System.currentTimeMillis();

        if(queryGraphPath == null)
            throw new RuntimeException("Query graph was not set in the config file");
        QueryGraph queryGraph = new QueryGraph(queryGraphPath);

        queryGraphBuildingTime = System.currentTimeMillis() - queryGraphBuildingTime;

        // This also broadcasts the data graph, which is in the closure of the configuration
        configBC = sc.broadcast(config);
        queryGraphBC = sc.broadcast(queryGraph);

        configBC.value().initialize();

        // Initializing the accumulator
        initAccums();

        // TODO need to broadcast the data graph and query graph, and probably also the Configuration singleton?
    }

    void initAccums() {
        aggAccums = new HashMap();

        aggAccums.put(TREE_BUILDING_INIT_START_TIME,
                sc.sc().collectionAccumulator(TREE_BUILDING_INIT_START_TIME));
        aggAccums.put(TREE_BUILDING_INIT_FINISH_TIME,
                sc.sc().collectionAccumulator(TREE_BUILDING_INIT_FINISH_TIME));
        aggAccums.put(TREE_BUILDING_COMPUTATION_START_TIME,
                sc.sc().collectionAccumulator(TREE_BUILDING_COMPUTATION_START_TIME));
        aggAccums.put(TREE_BUILDING_COMPUTATION_FINISH_TIME,
                sc.sc().collectionAccumulator(TREE_BUILDING_COMPUTATION_FINISH_TIME));

        aggAccums.put(EMBEDDING_ENUMERATION_INIT_START_TIME,
                sc.sc().collectionAccumulator(EMBEDDING_ENUMERATION_INIT_START_TIME));
        aggAccums.put(EMBEDDING_ENUMERATION_INIT_FINISH_TIME,
                sc.sc().collectionAccumulator(EMBEDDING_ENUMERATION_INIT_FINISH_TIME));
        aggAccums.put(EMBEDDING_ENUMERATION_COMPUTATION_START_TIME,
                sc.sc().collectionAccumulator(EMBEDDING_ENUMERATION_COMPUTATION_START_TIME));
        aggAccums.put(EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME,
                sc.sc().collectionAccumulator(EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME));
    }

    void calcAccums() {
        // times for TB: TreeBuilding
        long tbCompStartTime = Long.MAX_VALUE;
        long tbCompFinishTime = Long.MIN_VALUE;

        // times for EE: EmbeddingEnumeration
        long eeCompStartTime = Long.MAX_VALUE;
        long eeCompFinishTime = Long.MIN_VALUE;

        for(Map.Entry<String, CollectionAccumulator<Long>> accum : aggAccums.entrySet()) {
            CollectionAccumulator acc = accum.getValue();
            List<Long> list = (List<Long>)acc.value();
            String accumName = accum.getKey();

            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;

            for(Long num: list) {
                if (max < num)
                    max = num;
                if (min > num)
                    min = num;
            }

            if(accumName.equals(TREE_BUILDING_COMPUTATION_START_TIME))
                tbCompStartTime = min;
            if(accumName.equals(TREE_BUILDING_COMPUTATION_FINISH_TIME))
                tbCompFinishTime = max;
            if(accumName.equals(EMBEDDING_ENUMERATION_COMPUTATION_START_TIME))
                eeCompStartTime = min;
            if(accumName.equals(EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME))
                eeCompFinishTime = max;
        }

        shuffleTime = eeCompStartTime - tbCompFinishTime;

        treeBuildingComputationTime = tbCompFinishTime - tbCompStartTime;
        embeddingEnumerationComputationTime = eeCompFinishTime - eeCompStartTime;
        totalComputationTime = eeCompFinishTime - tbCompStartTime;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        init(args);

        long startTime = System.currentTimeMillis();

        process();

        totalComputationTime += queryGraphBuildingTime;
        computeTime = endComputeTime - startComputeTime;

        String dataGraphPath = config.getString(config.SEARCH_MAINGRAPH_PATH,config.SEARCH_MAINGRAPH_PATH_DEFAULT);
        String queryGraphPath = config.getString(config.SEARCH_QUERY_GRAPH_PATH,config.SEARCH_QUERY_GRAPH_PATH_DEFAULT);
        int numThreads = config.numPartitions();

        // Print fine grained computation time and data
        LOG.fatal("\n\n@DEBUG Stats-Results: {"
                + "\n@DEBUG\tdata = " + dataGraphPath
                + "\n@DEBUG\tquery = " + queryGraphPath
                + "\n@DEBUG\t#threads = " + numThreads
                + "\n@DEBUG\tDataGraphBuildingTime = " + dataGraphBuildingTime
                + "\n@DEBUG\tQueryGraphBuildingTime = " + queryGraphBuildingTime
                + "\n@DEBUG\tTreeBuildingComputationTime (Step #1) = " + treeBuildingComputationTime
                + "\n@DEBUG\tEmbeddingEnumerationComputationTime (Step #2) = " + embeddingEnumerationComputationTime
                + "\n@DEBUG\tShuffleTime = " + shuffleTime
                + "\n@DEBUG\tPureComputeTime = " + (totalComputationTime - shuffleTime)
                + "\n@DEBUG\tTotal computation time (Query Graph Building time + Longest Computation Time) = " + totalComputationTime
                + "\n@DEBUG\tTotal computation time (Execution time of forEachPartition) = " + computeTime
                + "\n@DEBUG\tTotal processing time (broadcast time + worker processing) = " + (System.currentTimeMillis() - startTime)
                + "\n@EndOfDEBUG } ================ End of Debug ================\n");

        return 0;
    }

    private void process() {

        // ######### STEP 1 ##########

        // create the partitions RDD
        JavaRDD globalRDD = sc.parallelize(new ArrayList<Tuple2<Integer, String>>(numPartitions), numPartitions).cache();

        globalRDD.setName("parallelize");
        // create the computation that will be executed by each partition
        // First step computation
        TreeBuilding step1 = new TreeBuilding(numPartitions, configBC, queryGraphBC, this.aggAccums);

        // pass the the first computation function to each partition to be executed
        JavaRDD<Tuple2<Integer, SearchDataTree>> step1Output = globalRDD.mapPartitionsWithIndex(step1,false);
        step1Output.setName("step1.mapPartitionsWithIndex");

        io.arabesque.conf.Configuration config = io.arabesque.conf.Configuration.get();
        double outlierPct;
        try {
            outlierPct = config.getDouble(config.SEARCH_OUTLIERS_PCT, config.SEARCH_OUTLIERS_PCT_DEFAULT);
        } catch (ClassCastException e){
            if (e.toString().contains("java.lang.Integer cannot be cast to java.lang.Double")){
                int outlierPctInt = config.getInteger(config.SEARCH_OUTLIERS_PCT, (int) config.SEARCH_OUTLIERS_PCT_DEFAULT);
                outlierPct = (double) outlierPctInt;
            }
            else {
                throw e;
            }
        }

        if (outlierPct == 0){
            startComputeTime = System.currentTimeMillis();
            step1Output.foreachPartition(x -> {});
            endComputeTime = System.currentTimeMillis();
        }
        else {
            // Now flatten the results
            JavaPairRDD<Integer, SearchDataTree> step1Flattened = step1Output.flatMapToPair(tuple -> {
                ArrayList list = new ArrayList();
                list.add(new Tuple2(tuple._1(), tuple._2()));
                return list.iterator();
            });

            step1Flattened.setName("step1.flatMapToPair");

            // ######### STEP 2 ##########

            // Second step computation
            EmbeddingEnumeration step2 = new EmbeddingEnumeration(configBC, queryGraphBC, this.aggAccums);
            // Now group the messages by the Id of the destination partition
            // and execute the second computation step
            JavaRDD step2Output = step1Flattened.groupByKey().mapPartitionsWithIndex(step2, false);
            step2Output.setName("step2.mapPartitionsWithIndex");
            // in case you want to store the results of the map function in memory
            // step2Results.persist(StorageLevel.MEMORY_ONLY());
            // Now execute the previous set of transformations
            startComputeTime = System.currentTimeMillis();
            step2Output.foreachPartition(x -> {});
            endComputeTime = System.currentTimeMillis();
        }

        // calculate the accumulators
        calcAccums();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new QfragRunner(), args));
    }
}