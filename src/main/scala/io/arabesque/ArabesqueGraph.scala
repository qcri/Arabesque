package io.arabesque

import io.arabesque.computation.SparkMasterExecutionEngine
import io.arabesque.conf.{SparkConfiguration, Configuration}

import io.arabesque.embedding.Embedding

import org.apache.spark.Logging
import org.apache.spark.SparkContext

/**
 * Arabesque graph for calling algorithms on
 */
class ArabesqueGraph(
    path: String,
    local: Boolean,
    arab: ArabesqueContext) extends Logging {

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
    config.set ("output_path", "Motifs_Output")
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
    config.set ("output_path", "FSM_Output")
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
    config.set ("output_path", "Triangles_Output")
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
    config.set ("output_path", "Cliques_Output")
    config.set ("arabesque.clique.maxsize", maxSize)
    config.set ("computation", "io.arabesque.gmlib.clique.CliqueComputation")
    cliques (config)
  }
}
