package io.arabesque.Compression

import org.apache.hadoop.io.Writable
import java.io._
import java.util.concurrent.ExecutorService

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.{StorageReader, StorageStats}
import io.arabesque.pattern.Pattern

/**
  * Created by ehussein on 6/28/17.
  */
abstract class BasicGRAMIStorage extends Writable with java.io.Externalizable {
  protected var storage:GRAMIDomainStorage = _
  protected var serializeAsReadOnly: Boolean = false

  protected def createDomainStorage(readOnly: Boolean): GRAMIDomainStorage =
    if (readOnly)
      new GRAMIDomainStorageReadOnly
    else
      new GRAMIDomainStorage

  def addEmbedding(embedding: Embedding): Unit

  def getPattern: Pattern

  def getReader(computation: Computation[Embedding], numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader

  def aggregate(embZip: BasicGRAMIStorage): Unit

  def getNumberOfDomains: Int = storage.getNumberOfDomains

  def getStorage: GRAMIDomainStorage = storage

  def getNumberOfEnumerations: Long = storage.getNumberOfEnumerations

  def finalizeConstruction(pool: ExecutorService, numParts: Int): Unit = {
    storage.finalizeConstruction(pool, numParts)
  }

  def clear(): Unit = {
    storage.clear()
  }

  def getStats: StorageStats = storage.getStats

  def getSerializeasWriteOnly: Boolean = serializeAsReadOnly

  def setSerializeAsReadOnly(serializeAsReadOnly: Boolean): Unit = {
    this.serializeAsReadOnly = serializeAsReadOnly
  }

  @throws[IOException]
  def writeInParts(outputs: Array[DataOutput], hasContent: Array[Boolean]): Unit = {
    storage.write(outputs, hasContent)
  }
}
