package io.arabesque.odag

import java.util.concurrent.ExecutorService
import java.io._

import io.arabesque.embedding.Embedding


class MultiPatternODAGStash
    extends BasicODAGStash[MultiPatternODAG, MultiPatternODAGStash] {
  
  override def addEmbedding(embedding: Embedding): Unit = ???

  override def aggregate(odag: MultiPatternODAG): Unit = ???

  override def aggregateUsingReusable(odag: MultiPatternODAG): Unit = ???

  override def aggregate(other: MultiPatternODAGStash): Unit = ???
   
  override def finalizeConstruction(
      pool: ExecutorService, parts: Int): Unit = ???

  override def isEmpty(): Boolean = ???

  override def getNumZips(): Int = ???

  override def clear(): Unit = ???

  override def readExternal(objInput: ObjectInput): Unit = ???

  override def writeExternal(objOutput: ObjectOutput): Unit = ???

  override def readFields(dataInput: DataInput): Unit = ???

  override def write(dataOutput: DataOutput): Unit = ???
}
