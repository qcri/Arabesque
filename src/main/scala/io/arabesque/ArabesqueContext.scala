package io.arabesque

import io.arabesque.computation.SparkMasterExecutionEngine

import org.apache.spark.Logging
import org.apache.spark.SparkContext

/**
 * Context for creating Arabesque Applications
 */
class ArabesqueContext(sc: SparkContext) extends Logging {

  def sparkContext: SparkContext = sc

  def textFile(path: String, local: Boolean = false): ArabesqueGraph = {
    new ArabesqueGraph (path, this)
  }

  def stop() = {
  }
}
