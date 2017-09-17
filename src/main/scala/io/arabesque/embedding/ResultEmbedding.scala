package io.arabesque.embedding

import java.io.DataOutput

import org.apache.hadoop.io.Writable


/**
  *
  */
trait ResultEmbedding extends Writable {
  def words: Array[Int]
  def combinations(k: Int): Iterator[ResultEmbedding]

  override def write(out: DataOutput): Unit = {
    out.writeInt (words.size)
    words.foreach (w => out.writeInt(w))
  }

  override def hashCode(): Int = {
    var result = this.words.length
    var i = 0
    while (i < this.words.length) {
      result = 31 * result + this.words(i)
      i += 1
    }
    result
  }

  override def equals(_other: Any): Boolean = {
    if (_other == null || getClass != _other.getClass) return false
    return equals (_other.asInstanceOf[ResultEmbedding])
  }

  def equals(other: ResultEmbedding): Boolean = {
    if (other == null) return false

    if (this.words.length != other.words.length) return false

    var i = 0
    while (i < this.words.length) {
      if (this.words(i) != other.words(i)) return false
      i += 1
    }

    return true
  }

  def toOutputString(): String = {
    return "[" + words.mkString(", ") + "]"
  }
}

object ResultEmbedding {
  def apply (strEmbedding: String) = {
    if (strEmbedding contains "-")
      EEmbedding (strEmbedding)
    else
      VEmbedding (strEmbedding)
  }
  def apply(embedding: Embedding) = {
    if (embedding.isInstanceOf[EdgeInducedEmbedding])
      EEmbedding (embedding.getEdges.toIntArray)
    else
      VEmbedding (embedding.getVertices.toIntArray)
  }
}
