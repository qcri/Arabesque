package io.arabesque.compression

import org.apache.hadoop.io.Writable
import java.io._
import java.util.concurrent.ExecutorService

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.{StorageReader, StorageStats}
import io.arabesque.pattern.Pattern
import io.arabesque.report.StorageReport
import io.arabesque.utils.Logging

/**
  * Created by ehussein on 6/28/17.
  */
abstract class ScalaSimpleStorage extends Writable with java.io.Externalizable with Logging {
  protected var storage:UPSDomainStorage = _
  protected var serializeAsReadOnly: Boolean = false

  protected def createDomainStorage(readOnly: Boolean): UPSDomainStorage =
    if (readOnly)
      new UPSDomainStorageReadOnly
    else
      new UPSDomainStorage

  def addEmbedding(embedding: Embedding): Unit

  def getPattern: Pattern

  def getReader(computation: Computation[Embedding], numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader

  def aggregate(embZip: ScalaSimpleStorage): Unit

  def getNumberOfDomains(): Int = storage.getNumberOfDomains

  def getStorage: UPSDomainStorage = storage

  def getNumberOfEnumerations: Long = storage.getNumberOfEnumerations

  def getNumberOfEmbeddings: Long = storage.getNumberOfEmbeddings

  def finalizeConstruction(pool: ExecutorService, numParts: Int): Unit = {
    storage.finalizeConstruction(pool, numParts)
  }

  def clear(): Unit = {
    storage.clear()
  }

  def getStats: StorageStats = storage.getStats

  def getSerializeAsWriteOnly: Boolean = serializeAsReadOnly

  def setSerializeAsReadOnly(serializeAsReadOnly: Boolean): Unit = {
    this.serializeAsReadOnly = serializeAsReadOnly
  }

  @throws[IOException]
  def writeInParts(outputs: Array[DataOutput], hasContent: Array[Boolean]): Unit = {
    storage.write(outputs, hasContent)
  }

  //def printAllEnumerations(filePath: String) = storage.printAllEnumerations(filePath)

  //def getStorageReport(): StorageReport = storage.getStorageReport()

  //def getNumberSpuriousEmbeddings: Long = storage.getNumberSpuriousEmbeddings
}
