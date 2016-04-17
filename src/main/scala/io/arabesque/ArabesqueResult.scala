package io.arabesque

import io.arabesque.computation.SparkMasterExecutionEngine
import io.arabesque.conf.SparkConfiguration
import io.arabesque.odag.ODAG
import io.arabesque.pattern.Pattern
import io.arabesque.embedding.Embedding
import io.arabesque.embedding.ResultEmbedding

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Results of an Arabesque computation.
 * TODO: create a function which extract the embeddings from the ODAGs. We must
 * be cautious of load imbalance when implementing this. That would not be the
 * same as retrieve the output embeddings, as we would have the embeddings
 * produced by every iteration, including the output ones.
 */
case class ArabesqueResult(
    sc: SparkContext,
    config: SparkConfiguration[_ <: Embedding]) extends Logging {

  /**
   * Lazy evaluation for the results
   */
  private var masterEngineOpt: Option[SparkMasterExecutionEngine] = None
  def masterEngine: SparkMasterExecutionEngine = masterEngineOpt match {
    case None =>
      logInfo (s"starting/computing master execution engine")
      val _masterEngine = new SparkMasterExecutionEngine(sc, config)
      _masterEngine.compute
      _masterEngine.finalize
      masterEngineOpt = Some(_masterEngine)
      _masterEngine
    case Some(_masterEngine) =>
      _masterEngine
  }

  /**
   * Output embeddings
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
  private var odagsOpt: Option[RDD[ODAG]] = None
  def odags: RDD[ODAG] = odagsOpt match {
    case None =>
      val _odags = masterEngine.getOdags
      odagsOpt = Some(_odags)
      _odags
    case Some(_odags) =>
      _odags
  }

  /**
   * Saves embeddings as sequence files (HDFS): [NullWritable,ResultEmbedding]
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
      config.set ("output_path", path)
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
  def set(key: String, value: Any): ArabesqueResult = {
    this.copy (sc = sc, config = config.withNewConfig (key,value))
  }
}
