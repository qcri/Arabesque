package io.arabesque.embedding

trait ResultEmbedding {
  def words: Array[_]
}

object ResultEmbedding {
  def apply (strEmbedding: String) = {
    if (strEmbedding contains "-")
      EEmbedding (strEmbedding)
    else
      VEmbedding (strEmbedding)
  }
}
