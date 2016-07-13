package io.arabesque.embedding

import java.io.DataInput


/**
  * A vertex induced embedding
  *
  * Current semantics: Array(a, b, c, d) returns the
  * embedding induced by the vertices {a, b, c, d}
  *
  * @param words integer array indicating the embedding vertices
  */
case class VEmbedding(var words: Array[Int]) extends ResultEmbedding {

  // must have because we are messing around with Writables
  def this() = {
    this(null)
  }

  override def readFields(in: DataInput): Unit = {
    val wordsLen = in.readInt
    words = new Array[Int](wordsLen)
    for (i <- 0 until wordsLen) words(i) = in.readInt
  }

  override def toString = {
    s"VEmbedding(${words.mkString (", ")})"
  }

}

/**
  * A vertex induced embedding
  *
  */
object VEmbedding {
  def apply (strEmbedding: String) = {
    val vertices = (strEmbedding split "\\s+").
      map (_.toInt)
    new VEmbedding (vertices)
  }
}
