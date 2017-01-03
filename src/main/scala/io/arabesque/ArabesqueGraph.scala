package io.arabesque

import io.arabesque.utils.Logging

import java.util.UUID

import io.arabesque.computation._
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._

import scala.reflect.ClassTag

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

  private def resultHandler [E <: Embedding : ClassTag] (
      config: SparkConfiguration[E]): ArabesqueResult[E] = {
    new ArabesqueResult [E] (arab.sparkContext, config)
  }

  /** motifs */
  def motifs [E <: Embedding : ClassTag] (config: SparkConfiguration[E]): ArabesqueResult[E] = {
    resultHandler [E] (config)
  }

  /**
   * Computes all the motifs of a given size
   * {{{
    * import io.arabesque.ArabesqueContext
    *
    * val input_graph = "ArabesqueDir/data/cube.graph"
    * val max_size = 3
    *
    * val arab = new ArabesqueContext(sc)
    *
    * val graph = arab.textFile(input_graph)
    * val res = graph.motif(4)
    *
    * res.embeddings.count
    * res.embeddings.collect
    * }}}
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
    * Computes all the frequent subgraphs for the given support
    *
    * {{{
    * import io.arabesque.ArabesqueContext
    *
    * val input_graph = "ArabesqueDir/data/cube.graph"
    * val max_size = 2
    * val support = 3
    *
    * val graph = arab.textFile(input_graph)
    * val res = graph.fsm(support, max_size)
    *
    * res.embeddings.count
    * res.embeddings.collect
    *
    * }}}
    *
    *
    * @param support frequency threshold
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
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = graph.triangles()
    *
    *   // The cube graph has no triangle
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
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
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = graph.fsm()
    *
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
    *
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

  /** api for custom computations **/

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = arabGraph.
    *     edgeInducedComputation {(e,c) =>
    *       if (e.getNumWords == 3) {
    *         c.output (e)
    *       }
    *     }.
    *     withFilter ((e,c) => e.getNumWords == 3).
    *     withShouldExpand ((e,c) => e.getNumWords < 3)
    *
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
    *
    * @param process function that is called for each embedding produced
    *
    * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def edgeInducedComputation(process: (EdgeInducedEmbedding, Computation[EdgeInducedEmbedding]) => Unit)
      : ArabesqueResult[EdgeInducedEmbedding] = {
    val computation: Computation[EdgeInducedEmbedding] =
      new EComputationContainer(processOpt = Some(process))
    val config = new SparkConfiguration[EdgeInducedEmbedding].withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/custom-computation-${config.getUUID}")
    customComputation [EdgeInducedEmbedding] (config)
  }

  /**
   * Returns a new result with a configurable computation container.
   *
    * {{{
    *   import io.arabesque.ArabesqueContext
    *   val input_graph = "ArabesqueDir/data/cube.graph"
    *
    *   val graph = arab.textFile(input_graph)
    *   val res = arabGraph.
    *     vertexInducedComputation {(e,c) =>
    *       if (e.getNumWords == 3) {
    *         c.output (e)
    *       }
    *     }.
    *     withFilter ((e,c) => e.getNumWords == 3).
    *     withShouldExpand ((e,c) => e.getNumWords < 3)
    *
    *   res.embeddings.count()
    *   res.embeddings.collect()
    * }}}
    *
    * @param process function that is called for each embedding produced
    *
    * @return an [[io.arabesque.ArabesqueResult]] carrying odags and embeddings
   */
  def vertexInducedComputation(process: (VertexInducedEmbedding, Computation[VertexInducedEmbedding]) => Unit)
      : ArabesqueResult[VertexInducedEmbedding] = {
    val computation: Computation[VertexInducedEmbedding] =
      new VComputationContainer(processOpt = Some(process))
    val config = new SparkConfiguration[VertexInducedEmbedding].withNewComputation (computation)
    config.set ("input_graph_path", path)
    config.set ("input_graph_local", local)
    config.set ("output_path", s"${tmpPath}/custom-computation-${config.getUUID}")
    customComputation [VertexInducedEmbedding] (config)
  }

  def customComputation [E <: Embedding: ClassTag] (config: SparkConfiguration[E]): ArabesqueResult[E] = {
    resultHandler [E] (config)
  }
}
