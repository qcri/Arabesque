package io.arabesque.conf

import io.arabesque.computation._
import io.arabesque.conf.Configuration._
import io.arabesque.embedding.Embedding
import io.arabesque.graph.MainGraph
import io.arabesque.pattern.Pattern
import io.arabesque.utils.{Logging, SerializableConfiguration}

import org.apache.spark.SparkConf

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

import scala.collection.mutable.Map

/**
 * Configurations are passed along in this mapping
 */
case class SparkConfiguration[O <: Embedding](confs: Map[String,Any])
    extends Configuration[O] with Logging {

  def this() {
    this (Map.empty)
  }

  /**
   * Sets a configuration (mutable)
   */
  def set(key: String, value: Any): SparkConfiguration[O] = {
    confs.update (key, value)
    fixAssignments
    this
  }

  /**
   * Sets a configuration (mutable) if this configuration has not been set yet
   */
  def setIfUnset(key: String, value: Any): SparkConfiguration[O] = confs.get(key) match {
    case Some(_) =>
      this
    case None =>
      set (key, value)
  }

  /**
   * Sets a configuration (immutable)
   */
  def withNewConfig(key: String, value: Any): SparkConfiguration[O] = {
    val newConfig = this.copy [O] (confs = confs ++ Map(key -> value))
    newConfig.fixAssignments
    newConfig
  }

  /**
   * Sets a new computation in this configuration (immutable)
   */
  def withNewComputation [E <: Embedding] (computation: Computation[E]): SparkConfiguration[O] = {
    withNewConfig (SparkConfiguration.COMPUTATION_CONTAINER, computation)
  }
  
  /**
   * Sets a new master computation in this configuration (immutable)
   */
  def withNewMasterComputation (masterComputation: MasterComputation): SparkConfiguration[O] = {
    withNewConfig (SparkConfiguration.MASTER_COMPUTATION_CONTAINER, masterComputation)
  }

  /**
   * Sets a default hadoop configuration for this arabesque configuration. That
   * way both can be shipped together to the workers.
   * This function mutates the object.
   */
  def setHadoopConfig(conf: HadoopConfiguration): SparkConfiguration[O] = {
    val serHadoopConf = new SerializableConfiguration(conf)
    // we store the hadoop configuration as a common configuration
    this.confs.update (SparkConfiguration.HADOOP_CONF, serHadoopConf)
    this
  }

  /**
   * Returns a hadoop configuration assigned to this configuration, or throw an
   * exception otherwise.
   */
  def hadoopConf: HadoopConfiguration = confs.get(SparkConfiguration.HADOOP_CONF) match {
    case Some(serHadoopConf: SerializableConfiguration) =>
      serHadoopConf.value

    case Some(value) =>
      logError (s"The hadoop configuration type is invalid: ${value}")
      throw new RuntimeException(s"Invalid hadoop configuration type")

    case None =>
      logError ("The hadoop configuration type is not set")
      throw new RuntimeException(s"Hadoop configuration is not set")
  }

  /**
   * Translates Arabesque configuration into SparkConf.
   * ATENTION: This is highly spark-dependent
   */
  def sparkConf = {
    if (!isInitialized) {
      initialize()
    }
    val sparkMaster = getString ("spark_master", "local[*]")
    val conf = new SparkConf().
      setAppName ("Arabesque Master Execution Engine").
      setMaster (sparkMaster)
        
    conf.set ("spark.executor.memory", getString("worker_memory", "1g"))
    conf.set ("spark.driver.memory", getString("worker_memory", "1g"))

    sparkMaster match {
      case "yarn-client" | "yarn-cluster" | "yarn" =>
        conf.set ("spark.executor.instances", getInteger("num_workers", 1).toString)
        conf.set ("spark.executor.cores", getInteger("num_compute_threads", 1).toString)
        conf.set ("spark.driver.cores", getInteger("num_compute_threads", 1).toString)

      case standaloneUrl : String if standaloneUrl startsWith "spark://" =>
        conf.set ("spark.cores.max",
          (getInteger("num_workers", 1) * getInteger("num_compute_threads", 1)).toString)

      case _ =>
    }
    logInfo (s"Spark configurations:\n${conf.getAll.mkString("\n")}")
    conf
  }

  /**
   * This function accounts for the computation instance that can be set in
   * this configuration. If this is the case we just return the computation,
   * otherwise we create an extended one by calling the method from super.
   */
  override def createComputation[E <: Embedding](): Computation[E] = {
    confs.get(SparkConfiguration.COMPUTATION_CONTAINER) match {
      case Some(cc: ComputationContainer[_]) =>
        cc.shallowCopy().asInstanceOf[Computation[E]]

      case Some(c) =>
        throw new RuntimeException (s"Invalid computation type: ${c}")

      case None =>
        super.createComputation[E]()
    }
  }

  /**
   * Returns the computation container associated with this configuration, if
   * available. A computation container holds a custom computation that is
   * shipped to execution in the workers.
   */
  def computationContainer[E <: Embedding]: ComputationContainer[E] = {
    confs.get(SparkConfiguration.COMPUTATION_CONTAINER) match {
      case Some(cc: ComputationContainer[_]) =>
        cc.asInstanceOf[ComputationContainer[E]]
      case Some(cc) =>
        throw new RuntimeException (s"Computation ${cc} is not a container")
      case None =>
        throw new RuntimeException (s"No computation is set")
    }
  }

  /**
   * This function accounts for the master computation instance that can be set in
   * this configuration. If this is the case we just return the computation,
   * otherwise we create an extended one by calling the method from super.
   */
  override def createMasterComputation(): MasterComputation = {
    confs.get(SparkConfiguration.MASTER_COMPUTATION_CONTAINER) match {
      case Some(cc: MasterComputationContainer) =>
        cc.shallowCopy().asInstanceOf[MasterComputation]

      case Some(c) =>
        throw new RuntimeException (s"Invalid master computation type: ${c}")

      case None =>
        super.createMasterComputation()
    }
  }

  /**
   * Returns the master computation container associated with this configuration, if
   * available. A master computation container holds a custom computation that is
   * shipped to execution in the workers.
   */
  def masterComputationContainer: MasterComputationContainer = {
    confs.get(SparkConfiguration.MASTER_COMPUTATION_CONTAINER) match {
      case Some(cc: MasterComputationContainer) =>
        cc
      case _ =>
        new MasterComputationContainer()
    }
  }

  /**
   * We assume the number of requested executor cores as an alternative number of
   * partitions. However, by the time we call this function, the config *num_partitions*
   * should be already set by the user, or by the execution master engine which
   * has SparkContext.defaultParallelism as default
   */
  def numPartitions: Int = getInteger("num_partitions",
    getInteger("num_workers", 1) *
      getInteger("num_compute_threads", Runtime.getRuntime.availableProcessors))

  /**
   * Given the total number of partitions in the cluster, this function returns
   * roughly the number of partitions per worker. We assume an uniform division
   * among workers.
   */
  def numPartitionsPerWorker: Int = numPartitions / getInteger("num_workers", 1)

  /**
   * Update assign internal names to user defined properties
   */
  private def fixAssignments = {
    def updateIfExists(key: String, config: String) = confs.remove (key) match {
      case Some(value) => confs.update (config, value)
      case None =>
    }
    // log level
    updateIfExists ("log_level", CONF_LOG_LEVEL)
    
    // computation classes
    updateIfExists ("master_computation", CONF_MASTER_COMPUTATION_CLASS)
    updateIfExists ("computation", CONF_COMPUTATION_CLASS)

    // communication strategy
    updateIfExists ("comm_strategy", CONF_COMM_STRATEGY)

    // odag flush method
    updateIfExists ("flush_method", CONF_ODAG_FLUSH_METHOD)
    updateIfExists ("num_odag_parts", CONF_EZIP_AGGREGATORS)

    // input
    updateIfExists ("input_graph_path", CONF_MAINGRAPH_PATH)
    updateIfExists ("input_graph_subgraphs_path", CONF_MAINGRAPH_SUBGRAPHS_PATH)
    updateIfExists ("input_graph_local", CONF_MAINGRAPH_LOCAL)
 
    // output
    updateIfExists ("output_active", CONF_OUTPUT_ACTIVE)
    updateIfExists ("output_path", CONF_OUTPUT_PATH)

    // aggregation
    updateIfExists ("incremental_aggregation", CONF_INCREMENTAL_AGGREGATION)
   
    // max number of odags in case of odag communication strategy
    updateIfExists ("max_odags", CONF_COMM_STRATEGY_ODAGMP_MAX)

  }

  /**
   * Garantees that arabesque configuration is properly set
   *
   * TODO: generalize the initialization in the superclass Configuration
   */
  override def initialize(): Unit = synchronized {
    if (Configuration.isUnset || uuid != Configuration.get[SparkConfiguration[O]].uuid) {
      initializeInJvm()
      Configuration.set (this)
    }
  }

  /**
   * Called whether no arabesque configuration is set in the running jvm
   */
  private def initializeInJvm(): Unit = {

    fixAssignments

    // common configs
    setMainGraphClass (
      getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: MainGraph]]
    )

    setMasterComputationClass (
      getClass (CONF_MASTER_COMPUTATION_CLASS, CONF_MASTER_COMPUTATION_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: MasterComputation]]
    )
    
    setComputationClass (
      getClass (CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: Computation[O]]]
    )

    setPatternClass (
      getClass (CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT).
      asInstanceOf[Class[_ <: Pattern]]
    )

    setAggregationsMetadata (new java.util.HashMap())

    setOutputPath (getString(CONF_OUTPUT_PATH, CONF_OUTPUT_PATH_DEFAULT))
    
    // main graph
    if ( (getMainGraph() == null && initialized)
         || (getString ("spark_master", "local[*]") startsWith "local[")
         ) {
      logInfo ("Main graph is null, gonna read it")
      setMainGraph (createGraph())
    }

    initialized = true
  }

  def getValue(key: String, defaultValue: Any): Any = confs.get(key) match {
    case Some(value) => value
    case None => defaultValue
  }

  override def getInteger(key: String, defaultValue: Integer) =
    getValue(key, defaultValue).asInstanceOf[Int]

  override def getString(key: String, defaultValue: String) =
    getValue(key, defaultValue).asInstanceOf[String]
  
  override def getBoolean(key: String, defaultValue: java.lang.Boolean) =
    getValue(key, defaultValue).asInstanceOf[Boolean]

}

object SparkConfiguration {
  def get[E <: Embedding]: SparkConfiguration[E] = {
    Configuration.get[SparkConfiguration[E]].asInstanceOf[SparkConfiguration[E]]
  }

  // odag flush methods
  val FLUSH_BY_PATTERN = "flush_by_pattern" // good for regular distributions
  val FLUSH_BY_ENTRIES = "flush_by_entries" // good for irregular distributions but small embedding domains
  val FLUSH_BY_PARTS = "flush_by_parts"     // good for irregular distributions, period

  // communication strategies
  val COMM_ODAG_SP = "odag_sp"              // pack embeddings with single-pattern odags
  val COMM_ODAG_MP = "odag_mp"              // pack embeddings with multi-pattern odags
  val COMM_EMBEDDING = "embedding"          // pack embeddings with compressed caches (e.g., LZ4)

  // hadoop conf
  val HADOOP_CONF = "hadoop_conf"

  // computation container
  val COMPUTATION_CONTAINER = "computation_container"
  val MASTER_COMPUTATION_CONTAINER = "master_computation_container"
}
