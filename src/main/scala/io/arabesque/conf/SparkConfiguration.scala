package io.arabesque.conf

import io.arabesque.computation.{Computation, VertexInducedComputation}
import io.arabesque.embedding.{Embedding, VertexInducedEmbedding}
import io.arabesque.graph.{MainGraph, BasicMainGraph}
import io.arabesque.pattern.Pattern

import scala.collection.mutable.Map

/**
 * Configurations are passed along in this mapping
 */
class SparkConfiguration[O <: Embedding](confs: Map[String,String]) extends Configuration[O] {

  var numPartitions: Int = _

  // TODO: generalize the initialization
  override def initialize() {
    //super.initialize();

    // get config names
    // TODO: get theses configs from object mapping
    val graphClass = "io.arabesque.graph.BasicMainGraph"
    val embeddClass = "io.arabesque.embedding.VertexInducedEmbedding"
    val compClass = "io.arabesque.examples.motif.MotifComputation"
    val pattClass = "io.arabesque.pattern.JBlissPattern"
    val graphFile = confs.get("input_graph_path").get

    // common configs
    setMainGraphClass (Class.forName (graphClass).asInstanceOf[Class[MainGraph]])
    setEmbeddingClass(Class.forName (embeddClass).asInstanceOf[Class[O]])
    setComputationClass(Class.forName (compClass).asInstanceOf[Class[_ <: Computation[O]]])
    setPatternClass(Class.forName (pattClass).asInstanceOf[Class[_ <: Pattern]])

    // spark specific configs
    numPartitions = confs.getOrElse ("num_partitions", "10").toInt

    // main graph
    if (getMainGraph() == null) {
      try {
        setMainGraph(Configuration.get())
      } catch {
        case e: RuntimeException =>
          println (".main graph is null, gonna read it.")
          //setMainGraph(new BasicMainGraph (new org.apache.hadoop.fs.Path(graphFile), false, false))
          setMainGraph(new BasicMainGraph (java.nio.file.Paths.get(graphFile), false, false))
          Configuration.setIfUnset (this)
      }
    }
  }

  override def isOutputActive() = false

  override def getInteger(key: String, defaultValue: Integer) = confs.get(key) match {
    case Some(value) => value.toInt
    case None => defaultValue
  }

  override def toString = s"[sparkConf, mainGraphClass=${getMainGraphClass()}, embeddingClass=${getEmbeddingClass()}, computationClass=${getComputationClass()}]"
}
