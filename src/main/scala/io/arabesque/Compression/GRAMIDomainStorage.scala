package io.arabesque.Compression

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.{Storage, StorageReader, StorageStats}
import io.arabesque.pattern.Pattern
import io.arabesque.utils.WriterSetConsumer
import java.io.{DataInput, DataOutput, IOException}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by ehussein on 6/27/17.
  */
class GRAMIDomainStorage extends Storage[GRAMIDomainStorage] {
  protected var countsDirty: Boolean = false
  protected var keysOrdered: Boolean = false
  protected var domainEntries: ArrayBuffer[util.Set[Int]] = _
  protected var domain0OrderedKeys: Array[Int] = _
  protected var numberOfDomains: Int = -1
  protected var writerSetConsumer: WriterSetConsumer = new WriterSetConsumer

  // how many valid embeddings this storage actually have ?
  protected var numEmbeddings: Long = 0L

  def this(numberOfDomains: Int) {
    this()
    setNumberOfDomains(numberOfDomains)
  }

  @Override
  def addEmbedding(embedding: Embedding): Unit = {
    val numWords = embedding.getNumWords
    val words = embedding.getWords

    if (domainEntries.size != numWords)
      throw new RuntimeException(s"Tried to add an embedding with wrong number of expected vertices (${domainEntries.size}) ${embedding.toString}")

    var i = 0
    while(i < numWords) {
      domainEntries(i).add(words.getUnchecked(i))
      i += 1
    }

    countsDirty = true
    numEmbeddings += 1
  }

  @Override
  def aggregate(otherDomainStorage: GRAMIDomainStorage): Unit = {
    val otherNumberOfDomains = otherDomainStorage.numberOfDomains

    if (numberOfDomains == -1)
      setNumberOfDomains(otherNumberOfDomains)

    if (numberOfDomains != otherNumberOfDomains)
      throw new RuntimeException(s"Different number of domains: $numberOfDomains vs $otherNumberOfDomains")

    var i = 0
    while(i < numberOfDomains) {
      domainEntries(i).addAll(otherDomainStorage.domainEntries(i))
      i += 1
    }

    countsDirty = true
    numEmbeddings += otherDomainStorage.numEmbeddings
  }

  @Override
  def getNumberOfEnumerations: Long = {
    if (countsDirty) {
      /* ATTENTION: instead of an exception we return -1.
                  * This way we can identify whether the odags are ready or not to be
                  * read */
      //throw new RuntimeException("Should have never been the case");
      return -1
    }

    var num = 0

    if (domainEntries.size <= 0)
      return num


    /*
    for (domainEntry <- domainEntries.get(0).values) {
      num += domainEntry.getCounter
    }
    */
    // number of enumerations is a stat that we ared going to implement its calc later

    num
  }

  @Override
  def clear(): Unit = {
    if (domainEntries != null) {
      domainEntries.foreach(domain => domain.clear())
    }

    domain0OrderedKeys = null
    countsDirty = false
  }

  @Override
  def getStats: StorageStats = {
    val stats = new StorageStats

    stats.numDomains = domainEntries.size

    domainEntries.foreach(domain => {
      val domainSize = domain.size
      if (domainSize > stats.maxDomainSize) stats.maxDomainSize = domainSize
      if (domainSize < stats.minDomainSize) stats.minDomainSize = domainSize
      stats.sumDomainSize += domainSize

      // no pointers in grami so no pointer calcs
      /*
      for (domainEntry <- domainMap.values) {
        val numPointers = domainEntry.getNumPointers
        val numWastedPointers = domainEntry.getWastedPointers
        if (numPointers > stats.maxPointersSize) stats.maxPointersSize = numPointers
        if (numPointers < stats.minPointersSize) stats.minPointersSize = numPointers
        stats.sumPointersSize += numPointers
        stats.sumWastedPointers += numWastedPointers
      }
      */
    })

    stats
  }

  override def toString: String = toStringResume

  def getNumberOfDomains: Int = numberOfDomains

  @Override
  def toStringResume: String = {
    val sb = new StringBuilder
    sb.append(s"DomainStorage{numEmbeddings=$numEmbeddings, enumerations=$getNumberOfEnumerations,")

    var i = 0
    while(i < domainEntries.size) {
      sb.append(s" Domain[$i] size ${domainEntries.get(i).size}")

      if (i != domainEntries.size - 1)
        sb.append(", ")

      i += 1
    }
    sb.append("}")

    sb.toString
  }

  def toStringDebug: String = {
    val sb = new StringBuilder
    sb.append(s"DomainStorage{numEmbeddings=$numEmbeddings, enumerations=$getNumberOfEnumerations,")

    var i = 0
    while(i < domainEntries.size) {
      sb.append(s" Domain[$i] size ${domainEntries.get(i).size}\n")

      if(domainEntries.get(i).size != 0)
        sb.append("[\n")
      domainEntries.get(i).foreach(wordId => {
        sb.append(s"$wordId\n")
      })
      if(domainEntries.get(i).size != 0)
        sb.append("]\n")

      i += 1
    }
    sb.append("}")

    sb.toString
  }

  @Override
  @throws(classOf[RuntimeException])
  def getReader(pattern: Pattern, computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    throw new RuntimeException("Shouldn't be read")
  }

  @Override
  @throws(classOf[RuntimeException])
  def getReader(patterns: Array[Pattern], computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    throw new RuntimeException("Shouldn't be read")
  }

  @Override
  @throws(classOf[IOException])
  def readFields (dataInput: DataInput): Unit = {
    this.clear()
    numEmbeddings = dataInput.readLong()
    setNumberOfDomains(dataInput.readInt())

    var i = 0
    while(i < numberOfDomains) {
      val domainSize = dataInput.readInt()
      var j = 0

      while(j < domainSize) {
        domainEntries(i).add(dataInput.readInt())
        j += 1
      }
      i += 1
    }

    countsDirty = true
  }

  @Override
  @throws[IOException]
  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeLong(numEmbeddings)
    dataOutput.writeInt(numberOfDomains)

    domainEntries.foreach(domain => {
      dataOutput.writeInt(domain.size())
      domain.foreach(wordId => dataOutput.writeInt(wordId))
    })
  }

  @throws[IOException]
  def write(outputs: Array[DataOutput], hasContent: Array[Boolean]): Unit = {
    val numParts = outputs.length
    val numEntriesOfPartsInDomain = new Array[Int](numParts)

    var i = 0
    var partId = 0
    while (i < numParts) {
      outputs(i).writeLong(numEmbeddings)
      outputs(i).writeInt(numberOfDomains)
      i += 1
    }

    domainEntries.foreach(domain => {
      util.Arrays.fill(numEntriesOfPartsInDomain, 0)

      domain.foreach(wordId => {
        partId = wordId % numParts
        numEntriesOfPartsInDomain(partId) += 1
      })

      i = 0
      while (i < numParts) {
        val numEntriesOfPartInDomain = numEntriesOfPartsInDomain(i)
        outputs(i).writeInt(numEntriesOfPartInDomain)
        if (numEntriesOfPartInDomain > 0)
          hasContent(i) = true
        i += 1
      }

      domain.foreach(wordId => {
        partId = wordId % numParts
        outputs(partId).writeInt(wordId)
      })
    })
  }

  protected def setNumberOfDomains(numberOfDomains: Int): Unit = this.synchronized{
    if (numberOfDomains == this.numberOfDomains) return
    ensureCanStoreNDomains(numberOfDomains)
    this.numberOfDomains = numberOfDomains
  }

  private def ensureCanStoreNDomains(nDomains: Int): Unit = {
    if (nDomains < 0)
      return
    if (domainEntries == null)
      domainEntries = new ArrayBuffer[util.Set[Int]] (nDomains)

    val currentNumDomains = domainEntries.size
    val delta = nDomains - currentNumDomains
    var i = 0
    while (i < delta) {
      domainEntries += ConcurrentHashMap.newKeySet()
      i += 1
    }
  }

  def getDomainEntries: ArrayBuffer[util.Set[Int]] = {
    domainEntries
  }

  def getNumberOfEntries: Int = {
    var numEntries = 0

    domainEntries.foreach(domain => numEntries += domain.size )

    numEntries
  }

  def finalizeConstruction(): Unit = {
    val pool = Executors.newSingleThreadExecutor
    finalizeConstruction(pool, 1)
    pool.shutdown()
  }

  def finalizeConstruction(pool: ExecutorService, numParts: Int): Unit = this.synchronized {
    //recalculateCounts(pool, numParts)
    orderDomain0Keys()
  }

  private def orderDomain0Keys(): Unit = {
    if (domain0OrderedKeys != null && keysOrdered)
      return
    domain0OrderedKeys = domainEntries.get(0).toArray[Int]
    util.Arrays.sort(domain0OrderedKeys)
    keysOrdered = true
  }


  /*
  private class RecalculateTask(var partId: Int, var totalParts: Int) extends Runnable {
    private var domain = 0

    def setDomain(domain: Int): Unit = {
      this.domain = domain
    }

    override def run(): Unit = {
      val currentDomain = domainEntries.get(domain)
      val followingDomain = domainEntries.get(domain + 1)

      currentDomain.foreach(wordId => {
        if((wordId % totalParts) == partId) {
          // do nothing since this is grami
        }
      })
    }
  }
  */
}
