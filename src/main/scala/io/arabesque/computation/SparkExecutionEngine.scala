package io.arabesque.computation

import org.apache.spark.Logging
import org.apache.spark.{Accumulable, Accumulator}

import io.arabesque.conf.Configuration
import io.arabesque.embedding.Embedding
import io.arabesque.pattern.Pattern
import io.arabesque.odag.{ODAGStash, ODAG}
import io.arabesque.odag.ODAGStash._
import io.arabesque.odag.domain.DomainEntry

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.io.{DataOutput, ByteArrayOutputStream, DataOutputStream, OutputStream,
                DataInput, ByteArrayInputStream, DataInputStream, InputStream}

import scala.annotation.tailrec

/**
 */
class SparkExecutionEngine[O <: Embedding](
  partitionId: Int,
  superstep: Int,
  accums: Map[String,Accumulable[Map[Pattern,Long],(Pattern,Long)]],
  numEmbeddings: Accumulator[Long])
    extends CommonExecutionEngine[O] with Logging {

  // configuration has input parameters, computation knows how to ensure
  // arabesque's computational model
  var configuration: Configuration[O] = _
  var computation: Computation[O] = _

  // local configs
  var numBlocks: Int = _
  var maxBlockSize: Int = _
  
  // stashes
  var currentEmbeddingStash: ODAGStash = _
  var nextEmbeddingStash: ODAGStash = _
  var aggregatedEmbeddingStash: ODAGStash = _

  // stash efficient reader
  var odagStashReader: EfficientReader[O] = _

  // general usage pool
  var numPartitionsPerWorker: Int = _
  var pool: ExecutorService = _

  /**
   * Instantiates the computation, parameters and resources required to execute
   * the superstep in this partition
   */
  def init() = {
    configuration = Configuration.get()

    computation = configuration.createComputation()
    computation.init()

    nextEmbeddingStash = new ODAGStash()

    numBlocks = configuration.getInteger ("numBlocks", getNumberPartitions() * getNumberPartitions())
    maxBlockSize = configuration.getInteger ("maxBlockSize", 10000) // TODO: magic number ??


    numPartitionsPerWorker = getNumberPartitions()
    pool = Executors.newFixedThreadPool (numPartitionsPerWorker)

    computation.setUnderlyingExecutionEngine (this)
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    pool.shutdown()
  }

  /**
   * Realizes the computation of this module, i.e., expand/compute
   *
   * @param inboundStashes iterator of ODAG stashes
   */
  def compute(inboundStashes: Iterator[ODAGStash]) = expansionCompute (inboundStashes)

  /**
   * Iterates over ODAG stashes and call expansion/compute procedures on them.
   * It also bootstraps the cycle by requesting empty embedding from
   * configuration and expanding them.
   *
   * @param inboundStashes iterator of ODAG stashes
   */
  @tailrec
  private def expansionCompute(inboundStashes: Iterator[ODAGStash]): Unit = {
    if (superstep == 0) { // bootstrap

      val initialEmbedd: O = configuration.createEmbedding()
      computation.expand (initialEmbedd)

    } else {
      getNextInboundEmbedding (inboundStashes) match {
        case None =>

        case Some(embedding) =>
          internalCompute (embedding)
          // go for next embedding from stash
          expansionCompute (inboundStashes)

      }
    }
  }

  /**
   * Calls computation to expand an embedding
   *
   * @param embedding embedding to be expanded
   */
  def internalCompute(embedding: O) = computation.expand (embedding)

  /**
   * Reads next embedding from previous ODAGs
   *
   * @param remainingStashes iterator containing ODAG stashes which hold
   * compressed embeddings
   * @return some embedding or none
   */
  def getNextInboundEmbedding(remainingStashes: Iterator[ODAGStash]): Option[O] = {
    if (currentEmbeddingStash == null) {
      
      if (remainingStashes.hasNext) {

        currentEmbeddingStash = remainingStashes.next
        currentEmbeddingStash.finalizeConstruction (pool, numPartitionsPerWorker)

        // odag stashes have an efficient reader for compressed embeddings
        odagStashReader = new EfficientReader[O] (currentEmbeddingStash,
          computation,
          getNumberPartitions(),
          numBlocks,
          maxBlockSize)

      } else return None

    }

    // new embedding was found
    if (odagStashReader.hasNext) {

      Some(odagStashReader.next)

    // no more embeddings to be read from current stash, try to get another
    // stash by recursive call
    } else {

      currentEmbeddingStash = null
      getNextInboundEmbedding(remainingStashes)

    }
  }

  /**
   * Naively flushes outbound odags
   *
   * @return iterator of pairs of (pattern, odag)
   */
  def flush: Iterator[(Pattern,ODAG)]  = {
    // consume content in *nextEmbeddingStash*
    for (odag <- nextEmbeddingStash.getEzips().iterator)
      yield (odag.getPattern(), odag)
  }

  /** 
   *  Flushes outbound odags in parts, i.e., with single domain entries per odag
   *
   *  @return iterator of pairs of ((pattern,domainId,wordId), odag_with_one_entry)
   */
  def flushInParts: Iterator[((Pattern,Int,Int), ODAG)] = {

    /**
     * Iterator that split a big ODAG into small ODAGs containing only one entry
     * of the original. Thus, keyed by (pattern, domainId, wordId)
     */
    class ODAGPartsIterator(odag: ODAG) extends Iterator[((Pattern,Int,Int),ODAG)] {

      val domainIterator = odag.getStorage().getDomainEntries().iterator
      var domainId = -1
      var currEntriesIterator: Option[Iterator[(Integer,DomainEntry)]] = None

      val reusableOdag = new ODAG(odag.getPattern(), odag.getNumberOfDomains())

      @tailrec
      private def hasNextRec: Boolean = currEntriesIterator match {
        case None =>
          domainIterator.hasNext
        case Some(entriesIterator) if entriesIterator.isEmpty =>
          currEntriesIterator = None
          hasNextRec
        case Some(entriesIterator) =>
          entriesIterator.hasNext
      }

      override def hasNext = hasNextRec

      @tailrec
      private def nextRec: ((Pattern,Int,Int),ODAG) = currEntriesIterator match {

        case None => // set next domain and recursive call
          currEntriesIterator = Some(domainIterator.next.iterator)
          domainId += 1
          nextRec

        case Some(entriesIterator) => // format domain entry as new ODAG
          val newOdag = new ODAG(odag.getPattern(), odag.getNumberOfDomains())
          val (wordId, entry) = entriesIterator.next
          val domainEntries = newOdag.getStorage().getDomainEntries()

          domainEntries.get (domainId).put (wordId, entry)

          ((newOdag.getPattern(),domainId,wordId.intValue), newOdag)

      }

      override def next = nextRec
    }

    nextEmbeddingStash.getEzips.iterator.flatMap (new ODAGPartsIterator(_))
  }

  /**
   * Flushes outbound odags by chunks of bytes
   *
   * @return iterator of pairs ((pattern,partId), bytes)
   */
  def flushOutputs: Iterator[((Pattern,Int),Array[Byte])] = {

    val outputs = Array.fill[ByteArrayOutputStream](numPartitionsPerWorker)(new ByteArrayOutputStream())
    def createDataOutput(output: OutputStream): DataOutput = new DataOutputStream(output)
    val dataOutputs = outputs.map (output => createDataOutput(output))
    val hasContent = new Array[Boolean](numPartitionsPerWorker)

    val parts = ListBuffer.empty[((Pattern,Int),Array[Byte])]

    for (odag <- nextEmbeddingStash.getEzips().iterator) {
      // reset to reuse write streams
      for (i <- 0 until numPartitionsPerWorker) {
        outputs(i).reset
        hasContent(i) = false
      }

      // this method writes odag content into DataOutputs in parts
      odag.writeInParts (dataOutputs, hasContent)

      // attach to each byte array the corresponding key, i.e., (pattern, partId)
      (0 until numPartitionsPerWorker).foreach {partId =>
        if (hasContent(partId)) {
          val part = ((odag.getPattern(), partId), outputs(partId).toByteArray)
          parts += part
        }
      }
    }
    
    parts.iterator
  }

  /**
   * Called whenever an embedding survives the expand/filter process and must be
   * carried on to the next superstep
   *
   * @param embedding embedding that must be processed
   */
  def addOutboundEmbedding(embedding: O) = processExpansion (embedding)

  /**
   * Adds an expansion (embedding) to the outbound odags.
   *
   * @param expansion embedding to be added to the stash of outbound odags
   */
  override def processExpansion(expansion: O) = {
    nextEmbeddingStash.addEmbedding (expansion)
  }

  /**
   * TODO
   */
  override def getAggregatedValue[A <: Writable](name: String): A = {
    null.asInstanceOf[A]
  }

  /**
   * TODO Right now it adds to two accumulators: (1) counting by motif pattern
   * and (2) total of embeddings processed
   */
  override def map[K <: Writable, V <: Writable](name: String, key: K, value: V) = {
    accums.get(name) match {
      case Some(accum) =>
        numEmbeddings += 1
        accum += (key.asInstanceOf[Pattern], value.asInstanceOf[LongWritable].get())
    }
  }

  // other functions
  override def getPartitionId() = partitionId

  override def getNumberPartitions() = configuration.getInteger ("num_partitions", 10)

  override def getSuperstep() = superstep

  override def aggregate(name: String, value: LongWritable) = {
  }

  override def output(outputString: String) = {
    logInfo (outputString)
  }
}
