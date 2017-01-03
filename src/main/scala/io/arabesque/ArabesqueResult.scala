package io.arabesque

import io.arabesque.aggregation.{AggregationStorage, EndAggregationFunction}
import io.arabesque.aggregation.reductions.ReductionFunction
import io.arabesque.computation._
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding.{Embedding, ResultEmbedding}
import io.arabesque.odag.{SinglePatternODAG, BasicODAG}
import io.arabesque.pattern.Pattern
import io.arabesque.utils.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
 * Results of an Arabesque computation.
 * TODO: create a function which extract the embeddings from the ODAGs. We must
 * be cautious of load imbalance when implementing this. That would not be the
 * same as retrieve the output embeddings, as we would have the embeddings
 * produced by every iteration, including the output ones.
 */

/** Returns the result of an Arabesque computation
  *
  * @param sc a [[SparkContext]] instance
  * @param config spark configuration
  * @tparam E an embedding.
  */

case class ArabesqueResult [E <: Embedding : ClassTag] (
    sc: SparkContext,
    config: SparkConfiguration[E]) extends Logging {

  /**
   * Lazy evaluation for the results
   */
  private var masterEngineOpt: Option[SparkMasterEngine[E]] = None
  def masterEngine: SparkMasterEngine[E] = masterEngineOpt match {
    case None =>
      logInfo (s"starting/computing master execution engine")
      val _masterEngine = SparkMasterEngine [E] (sc, config)
      _masterEngine.compute
      _masterEngine.finalizeComputation
      masterEngineOpt = Some(_masterEngine)
      _masterEngine
    case Some(_masterEngine) =>
      _masterEngine
  }

  /**
   * Output: embeddings
   */
  private var embeddingsOpt: Option[RDD[ResultEmbedding]] = None
  def embeddings: RDD[ResultEmbedding] = embeddingsOpt match {
    case None if config.isOutputActive =>
      val _embeddings = masterEngine.getEmbeddings
      embeddingsOpt = Some(_embeddings)
      _embeddings
    case Some(_embeddings) if config.isOutputActive =>
      _embeddings
    case _ =>
      config.set ("output_active", true)
      masterEngineOpt = None
      embeddingsOpt = None
      odagsOpt = None
      embeddings
  }

  /**
   * ODAGs of all supersteps
   */
  private var odagsOpt: Option[RDD[_ <: BasicODAG]] = None
  def odags: RDD[_ <: BasicODAG] = odagsOpt match {
    case None =>
      val _odags = masterEngine.getOdags
      odagsOpt = Some(_odags)
      _odags
    case Some(_odags) =>
      _odags
  }

  /**
   * Registered aggregations
   */
  def registeredAggregations: Array[String] = {
    config.getAggregationsMetadata.map (_._1).toArray
  }

  /**
   * Get aggregations defined by the user or empty if it does not exist
   */
  def aggregation [K <: Writable, V <: Writable] (name: String): Map[K,V] = {
    val aggValue = masterEngine.
      getAggregatedValue [AggregationStorage[K,V]] (name)
    if (aggValue == null) Map.empty[K,V]
    else aggValue.getMapping
  }

  /**
   * Get all aggregations defined by the user
   */
  def aggregations
      : Map[String,Map[_ <: Writable, _ <: Writable]] = {
    Map (
      registeredAggregations.map (name => (name,aggregation(name))).toSeq: _*
    )
  }
  

  /**
   * Saves embeddings as sequence files (HDFS): [[org.apache.hadoop.io.NullWritable, ResultEmbedding]]
   * Behavior:
   *  - If at this point no computation was performed we just configure
   *  the execution engine and force the computation(count action)
   *  - Otherwise we rename the embeddings path to *path* and clear the
   *  embeddings RDD variable, which will force the creation of a new RDD with
   *  the corrected path.
   *
   * @param path hdfs(hdfs://) or local (file://) path
   */
  def saveEmbeddingsAsSequenceFile(path: String): Unit = embeddingsOpt match {
    case None =>
      logInfo ("no emebeddings found, computing them ... ")
      config.setOutputPath (path)
      embeddings.count

    case Some(_embeddings) =>
      logInfo (s"found results, renaming from ${config.getOutputPath} to ${path}")
      val fs = FileSystem.get(sc.hadoopConfiguration)
      fs.rename (new Path(config.getOutputPath), new Path(path))
      if (config.getOutputPath != path) embeddingsOpt = None
      config.setOutputPath (path)

  }

  /**
   * Saves the embeddings as text
   *
   * @param path hdfs(hdfs://) or local(file://) path
   */
  def saveEmbeddingsAsTextFile(path: String): Unit = {
    embeddings.
      map (emb => emb.words.mkString(" ")).
      saveAsTextFile (path)
  }

  /**
   * This function will handle to the user a new result with a new configuration
   *
   * @param key id of the configuration
   * @param value value of the new configuration
   *
   * @return new result
   */
  def set(key: String, value: Any): ArabesqueResult[E] = {
    this.copy (sc = sc, config = config.withNewConfig (key,value))
  }

  /**
   * Auxiliary function for handling computation containers that were not set in
   * this result
   */
  private def getComputationContainer [O <: Embedding]: ComputationContainer[O] = {
    try {
      config.computationContainer[O]
    } catch {
      case e: RuntimeException =>
        logError (s"No computation container was set." +
          s" Please start with 'edgeInducedComputation' or 'vertexInducedComputation' from ArabesqueGraph." +
          s" Error message: ${e.getMessage}")
        throw e
    }
  }

  /****** Arabesque Scala API: ComputationContainer ******/
  
  /**
   * Updates the process function of the underlying computation container.
   *
   * @param process process function to be applied to each embedding produced
   *
   * @return new result
   */
  def withProcess [O <: Embedding] (process: (O,Computation[O]) => Unit): ArabesqueResult[_] = {
    val newConfig = config.withNewComputation [O] (
      getComputationContainer[O].withNewFunctions (processOpt = Some(process)))
    this.copy (sc = sc, config = newConfig)
  }

  /**
   * Updates the filter function of the underlying computation container.
   *
   * @param filter filter function that determines whether embeddings must be
   * further processed or not.
   *
   * @return new result
   */
  def withFilter (filter: (E,Computation[E]) => Boolean): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (filterOpt = Some(filter)))
    this.copy (sc = sc, config = newConfig)
  }

  /**
   * Updates the shouldExpand function of the underlying computation container.
   *
   * @param shouldExpand function that determines whether the embeddings
   * produced must be extended or not.
   */
  def withShouldExpand (shouldExpand: (E,Computation[E]) => Boolean): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (shouldExpandOpt = Some(shouldExpand)))
    this.copy (sc = sc, config = newConfig)
  }
 
  /**
   * Updates the aggregationFilter function of the underlying computation
   * container.
   *
   * @param filter function that filters embeddings in the aggregation phase.
   *
   * @return new result
   */
  def withAggregationFilter (filter: (E,Computation[E]) => Boolean): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (aggregationFilterOpt = Some(filter)))
    this.copy (sc = sc, config = newConfig)
  }
  
  /**
   * Updates the aggregationFilter function regarding patterns instead of
   * embedding.
   *
   * @param filter function that filters embeddings of a given pattern in the
   * aggregation phase.
   *
   * @return new result
   */
  def withPatternAggregationFilter (filter: (Pattern,Computation[E]) => Boolean): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (pAggregationFilterOpt = Some(filter)))
    this.copy (sc = sc, config = newConfig)
  }
  
  /**
   * Updates the aggregationProcess function of the underlying computation
   * container.
   *
   * @param process function to be applied to each embedding in the aggregation
   * phase.
   *
   * @return new result
   */
  def withAggregationProcess (process: (E,Computation[E]) => Unit): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (aggregationProcessOpt = Some(process)))
    this.copy (sc = sc, config = newConfig)
  }
  
  /**
   * Updates the handleNoExpansions function of the underlying computation
   * container.
   *
   * @param func callback for embeddings that do not produce any expansion.
   *
   * @return new result
   */
  def withHandleNoExpansions (func: (E,Computation[E]) => Unit): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (handleNoExpansionsOpt = Some(func)))
    this.copy (sc = sc, config = newConfig)
  }
  
  /**
   * Updates the init function of the underlying computation
   * container.
   *
   * @param init initialization function for the computation
   *
   * @return new result
   */
  def withInit (init: (Computation[E]) => Unit): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (initOpt = Some(init)))
    this.copy (sc = sc, config = newConfig)
  }
  
  /**
   * Updates the initAggregations function of the underlying computation
   * container.
   *
   * @param initAggregations function that initializes the aggregations for the computation
   *
   * @return new result
   */
  def withInitAggregations (initAggregations: (Computation[E]) => Unit): ArabesqueResult[E] = {
    val newConfig = config.withNewComputation [E] (
      getComputationContainer[E].withNewFunctions (initAggregationsOpt = Some(initAggregations)))
    this.copy (sc = sc, config = newConfig)
  }
    
  /**
   * Adds a new aggregation to the computation
   *
   * @param name identifier of this new aggregation
   * @param keyClass class of the aggregation key
   * @param valueClass class of values being aggregated
   * @param persistent whether this aggregation must be persisted in each
   * superstep or not
   * @param reductionFunction the function that aggregates two values
   * @param endAggregationFunction the function that is applied at the end of
   * each local aggregation, in the workers
   *
   * @return new result
   */
  def withAggregation [K <: Writable, V <: Writable] (
      name: String,
      keyClass: Class[K],
      valueClass: Class[V],
      reductionFunction: ReductionFunction[V],
      endAggregationFunction: EndAggregationFunction[K,V] = null,
      persistent: Boolean = false): ArabesqueResult[E] = {

    // get the old init aggregations function in order to compose it
    val oldInitAggregation = getComputationContainer[E].initAggregationsOpt match {
      case Some(initAggregations) => initAggregations
      case None => (c: Computation[E]) => {}
    }

    // construct an incremental init aggregations function
    val initAggregations = (c: Computation[E]) => {
      oldInitAggregation (c) // init aggregations so far
      SparkConfiguration.get.registerAggregation (name, keyClass, valueClass,
        persistent, reductionFunction, endAggregationFunction)
    }

    withInitAggregations (initAggregations)

  }
  
  /****** Arabesque Scala API: MasterComputationContainer ******/
  
  /**
   * Updates the init function of the underlying master computation
   * container.
   *
   * @param init initialization function for the master computation
   *
   * @return new result
   */
  def withMasterInit (init: (MasterComputation) => Unit): ArabesqueResult[E] = {
    val newConfig = config.withNewMasterComputation (
      config.masterComputationContainer.withNewFunctions (initOpt = Some(init)))
    this.copy (sc = sc, config = newConfig)
  }
  
  /**
   * Updates the compute function of the underlying master computation
   * container.
   *
   * @param compute callback executed at the end of each superstep in the master
   *
   * @return new result
   */
  def withMasterCompute (compute: (MasterComputation) => Unit): ArabesqueResult[E] = {
    val newConfig = config.withNewMasterComputation (
      config.masterComputationContainer.withNewFunctions (computeOpt = Some(compute)))
    this.copy (sc = sc, config = newConfig)
  }

}
