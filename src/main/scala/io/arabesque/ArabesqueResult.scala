package io.arabesque

import io.arabesque.computation.SparkMasterExecutionEngine
import io.arabesque.conf.SparkConfiguration
import io.arabesque.odag.ODAG
import io.arabesque.pattern.Pattern
import io.arabesque.embedding.Embedding
import io.arabesque.embedding.ResultEmbedding

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
  private lazy val masterEngine: SparkMasterExecutionEngine = {
    val masterEngine = new SparkMasterExecutionEngine(sc, config)
    masterEngine.compute
    masterEngine.finalize
    masterEngine
  }

  /**
   * Output embeddings
   */
  lazy val embeddings: RDD[ResultEmbedding] = {
    masterEngine.getEmbeddings
  }

  /**
   * ODAGs of all supersteps
   */
  lazy val odags: RDD[ODAG] = {
    masterEngine.getOdags
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
