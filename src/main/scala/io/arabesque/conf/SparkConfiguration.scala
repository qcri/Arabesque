package io.arabesque.conf

import io.arabesque.computation.{Computation, MasterComputation}
import io.arabesque.conf.Configuration._
import io.arabesque.embedding.Embedding
import io.arabesque.graph.MainGraph
import io.arabesque.pattern.Pattern
import org.apache.spark.{Logging, SparkConf}

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
   * Sets a configuration (immutable)
   */
  def withNewConfig(key: String, value: Any): SparkConfiguration[O] = {
    val newConfig = this.copy [O] (confs = confs ++ Map(key -> value))
    newConfig.fixAssignments
    newConfig
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
      case "yarn-client" | "yarn-cluster" =>
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

    // input
    updateIfExists ("input_graph_path", CONF_MAINGRAPH_PATH)
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
  val FLUSH_BY_PATTERN = "flush_by_pattern" // good for regular distributions
  val FLUSH_BY_ENTRIES = "flush_by_entries" // good for irregular distributions but small embedding domains
  val FLUSH_BY_PARTS   = "flush_by_parts"   // good for irregular distributions, period
  val COMM_ODAG_SP = "odag_sp"              // pack embeddings with single-pattern odags
  val COMM_ODAG_MP = "odag_mp"              // pack embeddings with multi-pattern odags
  val COMM_EMBEDDING = "embedding"          // pack embeddings with compressed caches (e.g., LZ4)
}
