package io.arabesque

import java.util.UUID

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import org.apache.spark.Logging


/**
  *  Creates an [[io.arabesque.ArabesqueGraph]] used for calling arabesque graph algorithms
  *
  * @param path  a string indicating the path for input graph
  * @param local TODO
  * @param arab  an [[io.arabesque.ArabesqueContext]] instance
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

  private def resultHandler [E <: Embedding] (
      config: SparkConfiguration[E]): ArabesqueResult[E] = {
    new ArabesqueResult [E] (arab.sparkContext, config)
  }

  /** motifs */
  def motifs [E <: Embedding] (config: SparkConfiguration[E]): ArabesqueResult[E] = {
    resultHandler [E] (config)
  }

  /**
   * Computes all the motifs of a given size
   *
   * @param maxSize number of vertices of the target motifs
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def motifs(maxSize: Int): ArabesqueResult[_] = {
    Configuration.unset
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/motifs-${config.getUUID}")
    config.set ("arabesque.motif.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.motif.MotifComputation")
    motifs (config)
  }

  /** fsm */
  def fsm(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult[_] = {
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
  def fsm(support: Int, maxSize: Int = Int.MaxValue): ArabesqueResult[_] = {
    val config = new SparkConfiguration [EdgeInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/fsm-${config.getUUID}")
    config.set ("arabesque.fsm.maxsize", maxSize)
    config.set ("arabesque.fsm.support", support)
    config.set ("computation", "io.arabesque.gmlib.fsm.FSMComputation")
    config.set ("master_computation", "io.arabesque.gmlib.fsm.FSMMasterComputation")
    fsm (config)
  }

  /** triangles */
  def triangles(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult[_] = {
    resultHandler (config)
  }

  /**
   * Counts triangles
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def triangles(): ArabesqueResult[_] = {
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/triangles-${config.getUUID}")
    config.set ("computation", "io.arabesque.gmlib.triangles.CountingTrianglesComputation")
    triangles (config)
  }

  /** cliques */
  def cliques(config: SparkConfiguration[_ <: Embedding]): ArabesqueResult[_] = {
    resultHandler (config)
  }

  /**
   * Computes graph cliques of a given size
   *
   * @param maxSize target clique size
   *
   * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def cliques(maxSize: Int): ArabesqueResult[_] = {
    val config = new SparkConfiguration [VertexInducedEmbedding]
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/cliques-${config.getUUID}")
    config.set ("arabesque.clique.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.clique.CliqueComputation")
    cliques (config)
  }
}
