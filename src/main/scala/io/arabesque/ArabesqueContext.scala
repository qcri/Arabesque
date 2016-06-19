package io.arabesque

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{Logging, SparkContext}

/**
 * Context for creating Arabesque Applications
 */
class ArabesqueContext(sc: SparkContext) extends Logging {

  private val uuid: UUID = UUID.randomUUID
  def tmpPath: String = s"/tmp/arabesque-${uuid}" // TODO: base dir as config

  def sparkContext: SparkContext = sc

  def textFile(path: String, local: Boolean = false): ArabesqueGraph = {
    new ArabesqueGraph (path, this)
  }

  def stop() = {
    val fs = FileSystem.get (sc.hadoopConfiguration)
    val res = fs.delete (new Path(tmpPath))
    logInfo (s"Removing arabesque temp directory: ${tmpPath} (${res})")
  }
}
