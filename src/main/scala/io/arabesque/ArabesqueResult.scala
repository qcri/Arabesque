package io.arabesque

import io.arabesque.computation.SparkMasterEngine
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding.{Embedding, ResultEmbedding}
import io.arabesque.odag.{SinglePatternODAG, BasicODAG}
import io.arabesque.aggregation.AggregationStorage
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import scala.collection.JavaConversions._
import scala.collection.mutable.Map

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

case class ArabesqueResult [E <: Embedding] (
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
}
