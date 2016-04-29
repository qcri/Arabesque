package io.arabesque.odag

import io.arabesque.embedding.Embedding
import io.arabesque.computation.Computation
import io.arabesque.odag.domain.StorageReader
import io.arabesque.pattern.Pattern

import io.arabesque.conf.{Configuration, SparkConfiguration}

import java.io._

import scala.collection.JavaConversions._

class MultiPatternODAG extends BasicODAG[MultiPatternODAG] {

  var patterns: Set[Pattern] = Set.empty
  @transient val reusablePattern: Pattern = config.createPattern

  def config: SparkConfiguration[_ <: Embedding] =
    Configuration.get[SparkConfiguration[_ <: Embedding]]

  override def addEmbedding(embedding: Embedding): Unit = {
    reusablePattern.setEmbedding (embedding)
    if (!(patterns contains reusablePattern))
      patterns += reusablePattern.copy

    storage.addEmbedding (embedding)
  }
   
  override def aggregate(other: MultiPatternODAG): Unit = {
    // do quick pattern union
    patterns = this.patterns union other.patterns
    // do domain aggregation (like single pattern)
    storage.aggregate(other.storage)
  }

  override def getReader(
      computation: Computation[Embedding],
      numPartitions: Int,
      numBlocks: Int,
      maxBlockSize: Int): StorageReader = {
    storage.getReader(patterns.toArray, computation, numPartitions, numBlocks, maxBlockSize)
  }

  override def readExternal(objInput: ObjectInput): Unit = {
    readFields (objInput)
  }

  override def writeExternal(objOuptut: ObjectOutput): Unit = {
    write (objOuptut)
  }
  
  override def readFields(dataInput: DataInput): Unit = {
    this.clear
    val numPatterns = dataInput.readInt
    for (i <- 0 until numPatterns) {
      val pattern = config.createPattern
      pattern.readFields (dataInput)
      patterns += pattern
    }
    storage.readFields(dataInput)
  }

  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeInt (patterns.size)
    patterns.foreach (pattern => pattern.write(dataOutput))
    storage.write(dataOutput)
  }

}
