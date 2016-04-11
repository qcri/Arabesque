package io.arabesque.embedding

import io.arabesque.graph.Vertex

case class VEmbedding(words: Array[Vertex]) extends ResultEmbedding {
  override def toString = {
    s"VEmbedding(${words.mkString (", ")}"
  }
}

object VEmbedding {
  def apply (strEmbedding: String) = {
    val vertices = (strEmbedding split "\\s+").
      map (w => new Vertex (w.toInt, w.toInt))
    new VEmbedding (vertices)
  }
}
