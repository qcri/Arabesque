package io.arabesque.embedding

import io.arabesque.graph.Edge

case class EEmbedding(words: Array[Edge]) extends ResultEmbedding {
  override def toString = {
    s"EEmbedding(${words.mkString (", ")}"
  }
}

object EEmbedding {
  def apply (strEmbedding: String) = {
    val edgesStr = strEmbedding split "\\s+"
    val edges = new Array[Edge](edgesStr.size)
    for (i <- 0 until edges.size) {
      val words = (edgesStr(i) split "-").map (_.toInt)
      edges(i) = new Edge(words(0), words(1))
    }

    new EEmbedding (edges)
  }
}
