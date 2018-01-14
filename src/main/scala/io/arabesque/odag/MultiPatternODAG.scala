package io.arabesque.odag

import io.arabesque.embedding.Embedding
import io.arabesque.computation.Computation
import io.arabesque.odag.domain.PrimitiveDomainStorage
import io.arabesque.odag.domain.GenericDomainStorage
import io.arabesque.odag.domain.StorageReader
import io.arabesque.pattern.Pattern
import io.arabesque.conf.{Configuration, SparkConfiguration}

import java.io._

import scala.collection.JavaConversions._

class MultiPatternODAG extends BasicODAG {

  def config: SparkConfiguration[_ <: Embedding] =
    Configuration.get[SparkConfiguration[_ <: Embedding]]

  var patterns: Set[Pattern] = Set.empty
  @transient val reusablePattern: Pattern = config.createPattern

  def this(numDomains: Int) = {
    this()

    val commStrategy = Configuration.get[Configuration[Embedding]]().getCommStrategy()

    if (commStrategy.equals(SparkConfiguration.COMM_ODAG_SP) || commStrategy.equals(SparkConfiguration.COMM_ODAG_SP_PRIM))
      storage = new PrimitiveDomainStorage(numDomains)
    else
      storage = new GenericDomainStorage(numDomains)

    serializeAsReadOnly = false
  }

  def this(readOnly: Boolean) = {
    this()
    serializeAsReadOnly = false
    storage = createDomainStorage (readOnly)
  }

  def aggregationFilter(computation: Computation[_]): Boolean = {
    patterns = patterns.filter ( pattern =>
      computation.aggregationFilter (pattern)
      )
    !patterns.isEmpty
  }

  override def addEmbedding(embedding: Embedding): Unit = {
    reusablePattern.setEmbedding (embedding)
    addEmbedding (embedding, reusablePattern)
  }

  def addEmbedding(embedding: Embedding, pattern: Pattern): Unit = {
    if (!(patterns contains pattern))
      patterns += pattern.copy

    storage.addEmbedding (embedding)
  }

  override def getPattern: Pattern = ???
   
  override def aggregate(other: BasicODAG): Unit = other match {
    case mpOdag: MultiPatternODAG =>
      // do quick pattern union
      patterns = this.patterns union mpOdag.patterns
      // do domain aggregation (like single pattern)
      if(storage.isInstanceOf[PrimitiveDomainStorage]) {
        val castedStorage = storage.asInstanceOf[PrimitiveDomainStorage]
        castedStorage.aggregate(mpOdag.storage.asInstanceOf[PrimitiveDomainStorage])
      }
      else
        if(storage.isInstanceOf[GenericDomainStorage]) {
          val castedStorage = storage.asInstanceOf[GenericDomainStorage]
          castedStorage.aggregate(mpOdag.storage.asInstanceOf[GenericDomainStorage])
        }
    case _ =>
      throw new RuntimeException (s"Must aggregate odags of the same type")
  }

  override def getReader(
      computation: Computation[Embedding],
      numPartitions: Int,
      numBlocks: Int,
      maxBlockSize: Int): StorageReader = {
    storage.getReader(patterns.toArray, computation, numPartitions, numBlocks, maxBlockSize)
  }

  override def readExternal(objInput: ObjectInput): Unit = {
    serializeAsReadOnly = objInput.readBoolean
    storage = createDomainStorage(serializeAsReadOnly)
    readFields (objInput)
  }

  override def writeExternal(objOuptut: ObjectOutput): Unit = {
    objOuptut.writeBoolean(serializeAsReadOnly)
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
