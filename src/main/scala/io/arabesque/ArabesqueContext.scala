package io.arabesque

import io.arabesque.utils.Logging

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

/**
  * Creates an Arabesque Context from a Spark Context
  *
  * Example of usage:
  * {{{
  * import io.arabesque.ArabesqueContext
  * val arab = new ArabesqueContext(sc)
  * arab: io.arabesque.ArabesqueContext = io.arabesque.ArabesqueContext@3a996bbc
  * }}}
  *
  * @param sc a [[SparkContext]] instance
  *
  * @return an [[io.arabesque.ArabesqueContext]]
  *
 */
class ArabesqueContext(sc: SparkContext, logLevel: String = "info") extends Logging {

  private val uuid: UUID = UUID.randomUUID
  def tmpPath: String = s"/tmp/arabesque-${uuid}" // TODO: base dir as config

  def sparkContext: SparkContext = sc

  /**
    *  Indicates the path for input graph
    *
    * {{{
    *  val file_path = "~/input.graph" // Set the input file path
    *  graph = arab.textFile(file_path)
    *  graph: io.arabesque.ArabesqueGraph = io.arabesque.ArabesqueGraph@2310a619
    * }}}
    *
    *
    * @param path a string indicating the path for input graph
    * @param local TODO: Describe local variable
    * @return an [[io.arabesque.ArabesqueGraph]]
    * @see [[https://github.com/viniciusvdias/Arabesque/blob/master/README.md Arabesque README]] for how to prepare the input file
    */
  def textFile(path: String, local: Boolean = false): ArabesqueGraph = {
    new ArabesqueGraph (path, local, this, logLevel)
  }

  /**
    * Stops an [[io.arabesque.ArabesqueContext]] application
    * {{{
    * arab.stop()
    * }}}
    */
  def stop() = {
    val fs = FileSystem.get (sc.hadoopConfiguration)
    val res = fs.delete (new Path(tmpPath))
    logInfo (s"Removing arabesque temp directory: ${tmpPath} (${res})")
  }
}
