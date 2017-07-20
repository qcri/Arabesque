package io.arabesque.Compression

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.{Storage, StorageReader, StorageStats}
import io.arabesque.pattern.Pattern
import io.arabesque.utils.WriterSetConsumer
import io.arabesque.utils.Logging
import io.arabesque.conf.Configuration

import java.io.{DataInput, DataOutput, IOException}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import org.apache.commons.configuration.tree.xpath.ConfigurationNodePointerFactory

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by ehussein on 6/27/17.
  */
class SimpleDomainStorage extends Storage[SimpleDomainStorage] with Logging {
  protected var countsDirty: Boolean = false
  protected var keysOrdered: Boolean = false
  //protected var domainEntries2: ArrayBuffer[util.Set[Int]] = _
  protected var domainEntries: util.ArrayList[ConcurrentHashMap[Int, Boolean]] = _
  protected var domainCounters: ArrayBuffer[Long] = _
  protected var domain0OrderedKeys: Array[Int] = _
  protected var numberOfDomains: Int = -1
  protected var writerSetConsumer: WriterSetConsumer = new WriterSetConsumer

  // how many valid embeddings this storage actually have ?
  protected var numEmbeddings: Long = 0L

  def this(numberOfDomains: Int) {
    this()
    setLogLevel(Configuration.get[Configuration[Embedding]]().getLogLevel)
    setNumberOfDomains(numberOfDomains)
  }

  @Override
  def addEmbedding(embedding: Embedding): Unit = {
    val numWords = embedding.getNumWords
    val words = embedding.getWords

    logInfo(s"Trying to add embedding ${embedding.toOutputString}")

    if (domainEntries.size != numWords)
      throw new RuntimeException(s"Tried to add an embedding with wrong number of expected vertices (${domainEntries.size}) ${embedding.toString}")

    var i = 0
    while(i < numWords) {
      domainEntries(i).put(words.getUnchecked(i), false)
      i += 1
    }

    logInfo(s"Embedding ${embedding.toOutputString} has been added successfully.")

    countsDirty = true
    numEmbeddings += 1
  }

  @Override
  def aggregate(otherDomainStorage: SimpleDomainStorage): Unit = {
    logInfo(s"Trying to aggregate DomainStorage: ${otherDomainStorage.toString}")

    val otherNumberOfDomains = otherDomainStorage.numberOfDomains

    if (numberOfDomains == -1)
      setNumberOfDomains(otherNumberOfDomains)

    if (numberOfDomains != otherNumberOfDomains)
      throw new RuntimeException(s"Different number of domains: $numberOfDomains vs $otherNumberOfDomains")

    var i = 0
    while(i < numberOfDomains) {
      domainEntries(i).putAll(otherDomainStorage.domainEntries(i))
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

    var num: Long = 1L
    var hasEnums: Boolean = false

    if (domainEntries.size <= 0)
      return 0


    /*
    for (domainEntry <- domainEntries.get(0).values) {
      num += domainEntry.getCounter
    }
    */
    // number of enumerations is a stat that we are going to implement its calc later
    domainEntries.foreach( domain => {
      if(domain.size() != 0)
        hasEnums = true
      num *= domain.size()
    })

    if(!hasEnums)
      0
    else
      num
  }

  @Override
  def clear(): Unit = {
    if (domainEntries != null) {
      domainEntries.foreach(domain => domain.clear())
    }

    if(domainCounters != null) {
      domainCounters = new ArrayBuffer[Long](numberOfDomains)
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

  override def toString: String = toStringDebug

  def getNumberOfDomains: Int = numberOfDomains

  @Override
  def toStringResume: String = {
    val sb = new StringBuilder
    sb.append(s"SimpleDomainStorage{numEmbeddings=$numEmbeddings, enumerations=$getNumberOfEnumerations,")

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
    sb.append(s"SimpleDomainStorage{numEmbeddings=$numEmbeddings, enumerations=$getNumberOfEnumerations,")

    var i = 0
    while(i < domainEntries.size) {
      sb.append(s" Domain[$i] size ${domainEntries(i).size}\n")

      if(domainEntries(i).size != 0)
        sb.append("[\n")

      domainEntries(i).foreach(entry => {
        sb.append(s"${entry._1}\n") // print the wordId
      })

      if(domainEntries(i).size != 0)
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
        domainEntries(i).put(dataInput.readInt(), dataInput.readBoolean())
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
      domain.foreach(domainEntry => {
        // write wordId
        dataOutput.writeInt(domainEntry._1)
        // write associated value
        dataOutput.writeBoolean(domainEntry._2)
      })
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

      domain.foreach(domainEntry => {
        partId = domainEntry._1 % numParts
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

      domain.foreach(domainEntry => {
        partId = domainEntry._1 % numParts
        // write wordId
        outputs(partId).writeInt(domainEntry._1)
        // write associated value
        outputs(partId).writeBoolean(domainEntry._2)
      })
    })
  }

  protected def setNumberOfDomains(numberOfDomains: Int): Unit = this.synchronized{
    if (numberOfDomains == this.numberOfDomains)
      return
    ensureCanStoreNDomains(numberOfDomains)
    this.numberOfDomains = numberOfDomains
  }

  private def ensureCanStoreNDomains(nDomains: Int): Unit = {
    if (nDomains < 0)
      return
    if (domainEntries == null)
      domainEntries = new util.ArrayList[ConcurrentHashMap[Int, Boolean]](nDomains)

    if (domainCounters == null)
      domainCounters = new ArrayBuffer[Long](nDomains)

    val currentNumDomains = domainEntries.size
    val delta = nDomains - currentNumDomains
    var i = 0

    while (i < delta) {
      domainEntries += new ConcurrentHashMap[Int, Boolean]
      domainCounters += 0
      i += 1
    }

    if (delta > 0) {
      var counts = new ArrayBuffer[Long](nDomains)
      counts ++= domainCounters
      domainCounters = counts
    }
  }

  def getDomainEntries: util.ArrayList[ConcurrentHashMap[Int, Boolean]] = {
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

  // we need to implement recalculateCounts (i.e. recalcCounters)
  def finalizeConstruction(pool: ExecutorService, numParts: Int): Unit = this.synchronized {
    //recalculateCounts(pool, numParts)
    recalculateCounters()
    orderDomain0Keys()
  }

  private def recalculateCounters(): Unit = {
    if (!countsDirty || numberOfDomains == 0)
      return

    var i = numberOfDomains - 2

    // update the counter of the last domain with one
    domainCounters(numberOfDomains - 1) = 1

    while(i >= 0) {
      // since the counters are the cartesian products of the sizes of the following domains
      // then currentCounter = lastDomain.counter * lastDomain.size
      domainCounters(i) = domainCounters(i + 1) * domainEntries(i + 1).size()
      i -= 1
    }

    countsDirty = false
  }

  private def orderDomain0Keys(): Unit = {
    if (domain0OrderedKeys != null && keysOrdered)
      return
    domain0OrderedKeys = domainEntries.get(0).keys().toArray[Int]
    util.Arrays.sort(domain0OrderedKeys)
    keysOrdered = true
  }

  protected def getWordIdsOfDomain(domainId: Int) : Array[Int] = {
    if(domainId >= numberOfDomains || domainId < 0)
      throw new ArrayIndexOutOfBoundsException(s"Should not access domain $domainId while numOfDomain=$numberOfDomains")
    val keys: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val keysSet = domainEntries(domainId).keys()

    while(keysSet.hasMoreElements)
      keys.add(keysSet.nextElement())

    keys.toArray
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
