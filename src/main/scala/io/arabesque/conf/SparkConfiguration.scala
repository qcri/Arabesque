package io.arabesque.conf

import io.arabesque.conf.Configuration._
import io.arabesque.computation.{Computation, MasterComputation}
import io.arabesque.embedding.Embedding
import io.arabesque.graph.{MainGraph, BasicMainGraph}
import io.arabesque.pattern.Pattern

import org.apache.spark.SparkConf

import scala.collection.mutable.Map

import scala.collection.JavaConversions._

/**
 * Configurations are passed along in this mapping
 */
class SparkConfiguration[O <: Embedding](confs: Map[String,Any]) extends Configuration[O] {

  /**
   * Translates Arabesque configuration into SparkConf
   */
  def nativeSparkConf = {
    assert (initialized)
    val sparkMaster = getString ("spark_master", "local[*]")
    val conf = new SparkConf().
      setAppName ("Arabesque Master Execution Engine").
      setMaster (sparkMaster)

    sparkMaster match {
      case "yarn-client" | "yarn-cluster" =>
        conf.set ("spark.executor.instances", getString("num_workers", "1"))
        conf.set ("spark.executor.cores", getString("num_compute_threads", "1"))
      case standaloneUrl : String if standaloneUrl startsWith "spark://" =>
        conf.set ("spark.cores.max",
          (getInteger("num_workers", 1) * getInteger("num_compute_threads", 1)).toString)
      case _ =>
    }
    conf
  }

  /**
   * Update assign internal names to user defined properties
   */
  def fixAssignments = {
    def updateIfExists(key: String, config: String) = confs.remove (key) match {
      case Some(value) => confs.update (config, value)
      case None =>
    }
    
    // computation classes
    updateIfExists ("master_computation", CONF_MASTER_COMPUTATION_CLASS)
    updateIfExists ("computation", CONF_COMPUTATION_CLASS)

    // input
    updateIfExists ("input_graph_path", CONF_MAINGRAPH_PATH)
    updateIfExists ("input_graph_local", CONF_MAINGRAPH_LOCAL)
 
    // output
    updateIfExists ("output_active", CONF_OUTPUT_ACTIVE)
    updateIfExists ("output_path", CONF_OUTPUT_PATH)

  }

  // TODO: generalize the initialization
  override def initialize() {
    fixAssignments

    // common configs
    setMainGraphClass (
      getClass (CONF_MAINGRAPH_CLASS, CONF_MAINGRAPH_CLASS_DEFAULT).asInstanceOf[Class[_ <: MainGraph]]
    )

    setMasterComputationClass (
      getClass (CONF_MASTER_COMPUTATION_CLASS, CONF_MASTER_COMPUTATION_CLASS_DEFAULT).asInstanceOf[Class[_ <: MasterComputation]]
    )
    
    setComputationClass (
      getClass (CONF_COMPUTATION_CLASS, CONF_COMPUTATION_CLASS_DEFAULT).asInstanceOf[Class[_ <: Computation[O]]]
    )

    setPatternClass (
      getClass (CONF_PATTERN_CLASS, CONF_PATTERN_CLASS_DEFAULT).asInstanceOf[Class[_ <: Pattern]]
    )

    setAggregationsMetadata (new java.util.HashMap())

    // main graph
    if (getMainGraph() == null) {
      try {
        setMainGraph(Configuration.get[Configuration[O]].getMainGraph())
      } catch {
        case e: RuntimeException =>
          println (".main graph is null, gonna read it.")
          setMainGraph (createGraph())
      }
    }
    Configuration.setIfUnset (this)

    initialized = true
  }

  override def isOutputActive() = false

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

  override def toString = s"[sparkConf, mainGraphClass=${getMainGraphClass()}, embeddingClass=${getEmbeddingClass()}, computationClass=${getComputationClass()}]"
}

object SparkConfiguration {
  val FLUSH_BY_PATTERN = "flush_by_pattern" // good for regular distributions
  val FLUSH_BY_ENTRIES = "flush_by_entries" // good for irregular distributions but small embedding domains
  val FLUSH_BY_PARTS   = "flush_by_parts" // good for irregular distributions, period
}
