package io.arabesque

import java.util.UUID

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding.Embedding
import org.apache.spark.Logging

/**
 * Arabesque graph for calling algorithms on
 */
class ArabesqueGraph(
    path: String,
    local: Boolean,
    arab: ArabesqueContext) extends Logging {

  private val uuid: UUID = UUID.randomUUID
  def tmpPath: String = s"${arab.tmpPath}/graph-${uuid}"

  def this(path: String, arab: ArabesqueContext) = {
    this (path, false, arab)
  }

  private def resultHandler(
      config: SparkConfiguration[_ <: Embedding]): ArabesqueResult = {
    new ArabesqueResult(arab.sparkContext, config)
  }

  /** motifs */
  def motifs(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult = {
    resultHandler (config)
  }

  /**
   * Computes all the motifs of a given size
   *
   * @param maxSize number of vertices of the target motifs
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def motifs(maxSize: Int): ArabesqueResult = {
    Configuration.unset
    val config = new SparkConfiguration
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/motifs-${config.getUUID}")
    config.set ("arabesque.motif.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.motif.MotifComputation")
    motifs (config)
  }

  /** fsm */
  def fsm(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult = {
    resultHandler (config)
  }

  /**
   * Computes the frequent subgraphs according to a support
   *
   * @param support threshold of frequency
   * @param maxSize upper bound for embedding exploration
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def fsm(support: Int, maxSize: Int = Int.MaxValue): ArabesqueResult = {
    val config = new SparkConfiguration
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/fsm-${config.getUUID}")
    config.set ("arabesque.fsm.maxsize", maxSize)
    config.set ("arabesque.fsm.support", support)
    config.set ("computation", "io.arabesque.gmlib.fsm.FSMComputation")
    fsm (config)
  }

  /** triangles */
  def triangles(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult = {
    resultHandler (config)
  }

  /**
   * Counts triangles
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def triangles(): ArabesqueResult = {
    val config = new SparkConfiguration
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/triangles-${config.getUUID}")
    config.set ("computation", "io.arabesque.gmlib.triangles.CountingTrianglesComputation")
    triangles (config)
  }

  /** cliques */
  def cliques(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult = {
    resultHandler (config)
  }

  /**
   * Computes graph cliques of a given size
   *
   * @param maxSize target clique size
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def cliques(maxSize: Int): ArabesqueResult = {
    val config = new SparkConfiguration
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/cliques-${config.getUUID}")
    config.set ("arabesque.clique.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.clique.CliqueComputation")
    cliques (config)
  }
}
