package io.arabesque.compression

import java.io.{DataInput, DataOutput, ObjectInput, ObjectOutput}

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.StorageReader
import io.arabesque.pattern.Pattern

import java.io._

/**
  * Created by ehussein on 6/28/17.
  */
class SinglePatternSimpleStorage extends SimpleStorage {
  private var pattern:Pattern = _

  def this(pattern: Pattern, numberOfDomains: Int) {
    this()
    this.pattern = pattern
    serializeAsReadOnly = false
    storage = new UPSDomainStorage(numberOfDomains)
  }

  def this(readOnly: Boolean) {
    this()
    serializeAsReadOnly = false
    storage = createDomainStorage(readOnly)
  }

  override def addEmbedding(embedding: Embedding): Unit = {
    //logInfo(s"Trying to add embedding ${embedding.toOutputString}")
    if (pattern == null)
      throw new RuntimeException("Tried to add an embedding without letting embedding zip know about the pattern")
    storage.addEmbedding(embedding)
  }

  override def aggregate(embZip: SimpleStorage): Unit = {
    //logInfo(s"Trying to aggregate storage ${embZip.toString}")
    if (embZip != null)
      storage.aggregate(embZip.asInstanceOf[SinglePatternSimpleStorage].storage)
  }

  override def getReader(computation: Computation[Embedding], numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader =
    storage.getReader(pattern, computation, numPartitions, numBlocks, maxBlockSize)

  @throws[IOException]
  override def write(out: DataOutput): Unit = {
    storage.write(out)
  }

  @throws[IOException]
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeBoolean(serializeAsReadOnly)
    write(out)
  }

  @throws[IOException]
  override def readFields(in: DataInput): Unit = {
    this.clear()
    storage.readFields(in)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  override def readExternal(in: ObjectInput): Unit = {
    serializeAsReadOnly = in.readBoolean
    storage = createDomainStorage(serializeAsReadOnly)
    readFields(in)
  }

  def setPattern(pattern: Pattern): Unit = {
    this.pattern = pattern
  }

  override def getPattern: Pattern = pattern

  override def toString: String = {
    var str:String = "SinglePatternSimpleStorage("
    if (pattern != null)
      str += pattern.toString
    str += ")" + storage.toString
    str
  }
}
