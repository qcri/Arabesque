package io.arabesque.embedding

import java.io.DataInput


/** An edge induced embedding
  *
  * Current semantics: Array(a, b, c, d) returns
  * the embedding induced by the edges (a, b) (c, d)
  *
  * @param words integer array indicating the embedding edges
  */
case class EEmbedding(var words: Array[Int]) extends ResultEmbedding {

  // must have because we are messing around with Writables
  def this() = {
    this(null)
  }

  override def readFields(in: DataInput): Unit = {
    val wordsLen = in.readInt
    words = new Array [Int] (wordsLen)
    for (i <- 0 until wordsLen) words(i) = in.readInt
  }

  override def toString = {
    s"EEmbedding(${words.mkString (", ")})"
  }
}

/**
  * An edge induced embedding
  */
object EEmbedding {
  def apply (strEmbedding: String) = {
    val edgesStr = strEmbedding split "\\s+"
    val edges = new Array[Int](edgesStr.size * 2)
    for (i <- 0 until edges.size by 2) {
      val words = (edgesStr(i) split "-").map (_.toInt)
      edges(i) = words(0)
      edges(i+1) = words(1)
    }

    new EEmbedding (edges)
  }
}
