package io.arabesque.embedding

import io.arabesque.graph.Vertex
import java.io.DataOutput
import java.io.DataInput

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
    s"VEmbedding(${words.mkString (", ")}"
  }

}

object VEmbedding {
  def apply (strEmbedding: String) = {
    val vertices = (strEmbedding split "\\s+").
      map (_.toInt)
    new VEmbedding (vertices)
  }
}
