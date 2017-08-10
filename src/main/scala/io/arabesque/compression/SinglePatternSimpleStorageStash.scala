package io.arabesque.compression

import io.arabesque.conf.Configuration
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.StorageStats
import io.arabesque.pattern.Pattern
import org.apache.giraph.aggregators.BasicAggregator
import org.apache.log4j.Logger
import java.io._
import java.util
import java.util._
import java.util.concurrent.ExecutorService
import scala.collection.JavaConversions._

/**
  * Created by ehussein on 7/6/17.
  */
class SinglePatternSimpleStorageStash
  extends SimpleStorageStash[SinglePatternSimpleStorage,SinglePatternSimpleStorageStash] with Externalizable {
  private val LOG = Logger.getLogger(classOf[SinglePatternSimpleStorageStash])
  private var compressedEmbeddingsByPattern: Map[Pattern, SinglePatternSimpleStorage] = new util.HashMap[Pattern, SinglePatternSimpleStorage]()
  private var reusablePattern: Pattern = Configuration.get[Configuration[Embedding]]().createPattern()

  def this(storageByPattern: util.Map[Pattern, SinglePatternSimpleStorage]) = {
    this()
    this.compressedEmbeddingsByPattern = storageByPattern
    this.reusablePattern = Configuration.get[Configuration[Embedding]]().createPattern()
    /*
    if(reusablePattern == null)
      logInfo("reusablePattern == null at SinglePatternSimpleStorageStash.createPattern()")
    else
      logInfo("reusablePattern != null when SinglePatternSimpleStorageStash.createPattern()")
    */
  }

  @Override
  override def addEmbedding(embedding: Embedding): Unit = {
    //logInfo(s"Trying to add embedding ${embedding.toOutputString}")
    try {
      /*
      if(reusablePattern == null)
        logInfo("reusablePattern == null when SinglePatternSimpleStorageStash.addEmbedding()")
      else
        logInfo("reusablePattern != null when SinglePatternSimpleStorageStash.addEmbedding()")
      */
      reusablePattern.setEmbedding(embedding)
      var embeddingsZip = compressedEmbeddingsByPattern.get(reusablePattern)
      if (embeddingsZip == null) {
        val patternCopy = reusablePattern.copy
        embeddingsZip = new SinglePatternSimpleStorage(patternCopy, embedding.getNumWords)
        compressedEmbeddingsByPattern.put(patternCopy, embeddingsZip)
      }
      embeddingsZip.addEmbedding(embedding)
    } catch {
      case e: Exception =>
        LOG.error("Error adding embedding to simple stash", e)
        LOG.error("Embedding: " + embedding.toOutputString)
        LOG.error("Pattern: " + reusablePattern)
        throw new RuntimeException(e)
    }
  }

  @Override
  override def aggregate(ezip: SinglePatternSimpleStorage): Unit = {
    //logInfo(s"Trying to aggregate stash ${ezip.toString}")
    val pattern = ezip.getPattern
    val existingEzip = compressedEmbeddingsByPattern.get(pattern)
    if (existingEzip == null) // this is a new pattern storage
      compressedEmbeddingsByPattern.put(pattern, ezip)
    else // the pattern already exists in the stash, so aggregate it to the corresponding storage
      existingEzip.aggregate(ezip)
  }

  @Override
  override def aggregateUsingReusable(ezip: SinglePatternSimpleStorage): Unit = {
    //logInfo(s"Trying to aggregate stash.aggregateUsingReusable ${ezip.toString}")
    val pattern: Pattern = ezip.getPattern

    var existingEzip: SinglePatternSimpleStorage = compressedEmbeddingsByPattern.get(pattern)

    if (existingEzip == null) { // this is a new pattern storage
      val patternCopy: Pattern = pattern.copy()
      ezip.setPattern(patternCopy)
      existingEzip = new SinglePatternSimpleStorage(patternCopy, ezip.getNumberOfDomains())
      compressedEmbeddingsByPattern.put(patternCopy, existingEzip)
    }

    existingEzip.aggregate(ezip)
  }

  @Override
  override def aggregateStash(value: SinglePatternSimpleStorageStash): Unit = {
    //logInfo(s"Trying to aggregate stash.aggregateStash ${value.toStringDebug}")
    //logInfo(s"With ${this.toStringDebug}")
    for (otherCompressedEmbeddingsByPatternEntry <- value.compressedEmbeddingsByPattern.entrySet) {
      val pattern = otherCompressedEmbeddingsByPatternEntry.getKey
      val otherCompressedEmbeddings = otherCompressedEmbeddingsByPatternEntry.getValue
      val thisCompressedEmbeddings = compressedEmbeddingsByPattern.get(pattern)
      if (thisCompressedEmbeddings == null)
        compressedEmbeddingsByPattern.put(pattern, otherCompressedEmbeddings)
      else
        thisCompressedEmbeddings.aggregate(otherCompressedEmbeddings)
    }
  }

  @Override
  override def finalizeConstruction(pool: ExecutorService, parts: Int): Unit = {
    for (entry <- compressedEmbeddingsByPattern.entrySet) {
      val pattern = entry.getKey
      val simpleStorage = entry.getValue
      simpleStorage.setPattern(pattern)
      simpleStorage.finalizeConstruction(pool, parts)
    }
  }

  @Override
  @throws[IOException]
  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeInt(compressedEmbeddingsByPattern.size)
    for (shrunkEmbeddingsByPatternEntry <- compressedEmbeddingsByPattern.entrySet) {
      val pattern = shrunkEmbeddingsByPatternEntry.getKey
      pattern.write(dataOutput)
      val shrunkEmbeddings = shrunkEmbeddingsByPatternEntry.getValue
      shrunkEmbeddings.write(dataOutput)
    }
  }

  @Override
  @throws[IOException]
  override def writeExternal(objOutput: ObjectOutput): Unit = {
    write(objOutput)
  }

  @Override
  @throws[IOException]
  override def readFields(dataInput: DataInput): Unit = {
    compressedEmbeddingsByPattern.clear()
    val numEntries = dataInput.readInt
    var i = 0
    while (i < numEntries) {
      val pattern = Configuration.get[Configuration[Embedding]]().createPattern
      pattern.readFields(dataInput)
      val shrunkEmbeddings = new SinglePatternSimpleStorage(false)
      shrunkEmbeddings.setPattern(pattern)
      shrunkEmbeddings.readFields(dataInput)
      compressedEmbeddingsByPattern.put(pattern, shrunkEmbeddings)
      i += 1
    }
  }

  @Override
  @throws[IOException]
  @throws[ClassNotFoundException]
  override def readExternal(objInput: ObjectInput): Unit = {
    readFields(objInput)
  }

  @Override
  override def isEmpty: Boolean = compressedEmbeddingsByPattern.isEmpty

  @Override
  override def getNumZips: Int = compressedEmbeddingsByPattern.size

  @Override
  override def clear(): Unit = {
    compressedEmbeddingsByPattern.clear()
  }

  def getEzip(pattern: Pattern): SinglePatternSimpleStorage = compressedEmbeddingsByPattern.get(pattern)

  @Override
  override def getEzips(): Collection[SinglePatternSimpleStorage] = compressedEmbeddingsByPattern.values

  @Override
  override def toString: String = "SinglePatternSimpleStorageStash{" + "compressedEmbeddingsByPattern=" + compressedEmbeddingsByPattern + '}'

  def toStringResume: String = {
    var numDomainsZips = 0
    var numDomainsEnumerations: Long = 0L
    for (ezip <- compressedEmbeddingsByPattern.values) {
      numDomainsZips += 1
      numDomainsEnumerations += ezip.getNumberOfEnumerations
    }
    "SinglePatternSimpleStorageStash{" + "numZips=" + numDomainsZips + ", " + "numEnumerations=" + numDomainsEnumerations + ", " + "}"
  }

  def toStringDebug: String = {
    val sb = new StringBuilder
    val orderedMap = new util.TreeMap[String, SinglePatternSimpleStorage]

    for (entry <- compressedEmbeddingsByPattern.entrySet) {
      orderedMap.put(entry.getKey.toString, entry.getValue)
    }

    sb.append("SinglePatternSimpleStorageStash{\n")
    var totalSum: Long = 0L
    for (entry <- orderedMap.entrySet) {
      sb.append("=====\n")
      sb.append(entry.getKey)
      sb.append('\n')
      sb.append(entry.getValue.toString())
      sb.append('\n')
      totalSum += entry.getValue.getNumberOfEnumerations
    }
    sb.append("Total sum=")
    sb.append(totalSum)
    sb.append("\n}")
    sb.toString
  }

  def getDomainStorageStatsString: String = {
    val domainStorageStats = new StorageStats
    for (ezip <- compressedEmbeddingsByPattern.values) {
      domainStorageStats.aggregate(ezip.getStats)
    }
    domainStorageStats.toString + "\n" + domainStorageStats.getSizeEstimations
  }

  class Aggregator extends BasicAggregator[SinglePatternSimpleStorageStash] {
    override def aggregate(value: SinglePatternSimpleStorageStash): Unit = {
      getAggregatedValue.aggregateStash(value)
    }

    override def createInitialValue = new SinglePatternSimpleStorageStash
  }

  override def printAllEnumerations(filePath: String) = {
    var i = 0
    compressedEmbeddingsByPattern.values().foreach(storage => {
      storage.printAllEnumerations(s"${filePath}_storage$i")
      i += 1
    })
  }

  /*
  override def saveStorageReports(filePath: String) = {
    var i = 0
    val pw = new PrintWriter(new File(filePath))
    val str:StringBuilder = new StringBuilder

    compressedEmbeddingsByPattern.values().foreach(storage => {
      val report = storage.getStorageReport()
      report.storageId = i
      str.append(report.toString())
      i += 1
    })

    pw.println(s"Staaaaash:\n")
    pw.println(str.toString())

    pw.close()
  }
  */

  def getNumberSpuriousEmbeddings: Long = {
    var totalSpurious:Long = 0L

    compressedEmbeddingsByPattern.values().foreach(storage => {
      totalSpurious += storage.getNumberSpuriousEmbeddings
    })

    totalSpurious
  }
}

object SinglePatternSimpleStorageStash {
  def apply() = new SinglePatternSimpleStorageStash
}