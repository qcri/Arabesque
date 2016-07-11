package io.arabesque

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Logging, SparkContext}

/**
  * Context for creating Arabesque Applications
  *
  * @param sc a [[SparkContext]] instance
  *
  * @return an [[io.arabesque.ArabesqueContext]]
  *
 */
class ArabesqueContext(sc: SparkContext) extends Logging {

  private val uuid: UUID = UUID.randomUUID
  def tmpPath: String = s"/tmp/arabesque-${uuid}" // TODO: base dir as config

  def sparkContext: SparkContext = sc

  /**
    *  Set the input file path for arabesque to read
    *
    *  The file should be formatted as describe in [[https://github.com/viniciusvdias/Arabesque/blob/master/README.md Arabesque README]]
    *
    * @param path a string indicating the path for input graph
    * @param local TODO: Describe local variable
    * @return an [[io.arabesque.ArabesqueGraph]]
    */
  def textFile(path: String, local: Boolean = false): ArabesqueGraph = {
    new ArabesqueGraph (path, this)
  }

  /**
    * Stops an [[io.arabesque.ArabesqueContext]] application
    *
    */
  def stop() = {
    val fs = FileSystem.get (sc.hadoopConfiguration)
    val res = fs.delete (new Path(tmpPath))
    logInfo (s"Removing arabesque temp directory: ${tmpPath} (${res})")
  }
}
