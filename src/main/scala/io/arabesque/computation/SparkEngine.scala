package io.arabesque.computation

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding.Embedding
import io.arabesque.utils.Logging

trait SparkEngine [E <: Embedding] 
    extends CommonExecutionEngine[E] with Serializable with Logging {

  var computed = false

  setLogLevel (configuration.getLogLevel)

  // configuration has input parameters, computation knows how to ensure
  // arabesque's computational model
  @transient lazy val configuration: SparkConfiguration[E] = {
    val configuration = Configuration.get [SparkConfiguration[E]]
    configuration
  }

  /**
   * We assume the number of requested executor cores as the default number of
   * partitions
   */
  def getNumberPartitions: Int = configuration.numPartitions

}
