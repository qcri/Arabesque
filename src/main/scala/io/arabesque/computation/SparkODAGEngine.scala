package io.arabesque.computation

import java.io._
import java.util.concurrent.{ExecutorService, Executors}

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.odag.ODAGStash._
import io.arabesque.odag.domain.DomainEntry
import io.arabesque.odag.{ODAGStash, SinglePatternODAG}
import io.arabesque.pattern.Pattern
import io.arabesque.utils.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Writable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map}
import scala.reflect.ClassTag

/**
 * Underlying engine that runs Arabesque workers in Spark.
 * Each instance of this engine corresponds to a partition in Spark computation
 * model. Instances' lifetimes refer also to one superstep of computation due
 * RDD's immutability.
 */
case class SparkODAGEngine[O <: Embedding](
    partitionId: Int,
    superstep: Int,
    hadoopConf: SerializableConfiguration,
    accums: Map[String,Accumulator[_]],
    // TODO do not broadcast if user's code does not requires it
    previousAggregationsBc: Broadcast[_])
  extends SparkEngine[O] {

  // configuration has input parameters, computation knows how to ensure
  // arabesque's computational model
  @transient lazy val configuration: Configuration[O] = Configuration.get [Configuration[O]]
  @transient lazy val computation: Computation[O] = {
    val computation = configuration.createComputation [O]
    computation.setUnderlyingExecutionEngine (this)
    computation.init()
    computation.initAggregations()
    computation
  }

  // local configs
  var numBlocks: Int = _
  var maxBlockSize: Int = _

  // stashes
  var currentEmbeddingStash: ODAGStash = _
  var nextEmbeddingStash: ODAGStash = _

  // stash efficient reader
  var odagStashReader: EfficientReader[O] = _

  // general usage pool
  var numPartitionsPerWorker: Int = _
  var pool: ExecutorService = _

  // aggregation storages
  @transient lazy val aggregationStorageFactory = new AggregationStorageFactory
  var aggregationStorages: Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  // output
  @transient var embeddingWriterOpt: Option[SeqWriter] = None
  @transient var outputStreamOpt: Option[OutputStreamWriter] = None
  @transient lazy val outputPath: Path = new Path(configuration.getOutputPath)

  // to feed accumulators
  private var numEmbeddingsProcessed: Long = _
  private var numEmbeddingsGenerated: Long = _
  private var numEmbeddingsOutput: Long = _

  /**
   * Instantiates the computation, parameters and resources required to execute
   * the superstep in this partition
   */
  def init() = {
    // set log level (from spark logging)
    val logLevel = Level.toLevel (configuration.getLogLevel)
    Logger.getLogger(logName).setLevel (logLevel)

    if (configuration.getEmbeddingClass() == null)
      configuration.setEmbeddingClass (computation.getEmbeddingClass())

    nextEmbeddingStash = new ODAGStash()

    numBlocks = configuration.getInteger ("numBlocks",
      getNumberPartitions() * getNumberPartitions())
    maxBlockSize = configuration.getInteger ("maxBlockSize", 10000) // TODO: magic number ??

    numPartitionsPerWorker = configuration.getInteger ("num_odag_parts", getNumberPartitions())
    // TODO: the engine in Spark is volatile, maybe get this pool out of here
    // (maybe within configuration, which will be initialized once per JVM)
    pool = Executors.newFixedThreadPool (numPartitionsPerWorker)

    // aggregation storage
    aggregationStorages = Map.empty

    // accumulators
    numEmbeddingsProcessed = 0
    numEmbeddingsGenerated = 0
    numEmbeddingsOutput = 0
  }

  /**
   * Releases resources allocated for this instance
   */
  override def finalize() = {
    super.finalize()
    // make sure we close writers
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (embeddingWriterOpt.isDefined) embeddingWriterOpt.get.close
    pool.shutdown()
  }

  /**
   * Returns a new execution engine from this with the aggregations/computation
   * variables updated (immutability)
   *
   * @param aggregationsBc broadcast variable with aggregations
   * @return the new execution engine, ready for flushing
   */
  def withNewAggregations(aggregationsBc: Broadcast[_]): SparkODAGEngine[O] = {
    
    // we first get a copy of the this execution engine, with previous
    // aggregations updated
    val execEngine = this.copy [O] (
      previousAggregationsBc = aggregationsBc,
      accums = accums)

    // set next stash with odags
    execEngine.nextEmbeddingStash = nextEmbeddingStash
    
    execEngine
  }

  /**
   * It does the computation of this module, i.e., expand/compute
   *
   * @param inboundStashes iterator of SinglePatternODAG stashes
   */
  def compute(inboundStashes: Iterator[ODAGStash]) = {
    expansionCompute (inboundStashes)
    flushStatsAccumulators
  }

  /**
   * Iterates over SinglePatternODAG stashes and call expansion/compute procedures on them.
   * It also bootstraps the cycle by requesting empty embedding from
   * configuration and expanding them.
   *
   * @param inboundStashes iterator of SinglePatternODAG stashes
   */
  private def expansionCompute(inboundStashes: Iterator[ODAGStash]): Unit = {
    if (superstep == 0) { // bootstrap

      val initialEmbedd: O = configuration.createEmbedding()
      computation.expand (initialEmbedd)

    } else {
      var hasNext = true
      while (hasNext) getNextInboundEmbedding (inboundStashes) match {
        case None =>
          hasNext = false

        case Some(embedding) =>
          internalCompute (embedding)
          numEmbeddingsProcessed += 1

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
   * @param remainingStashes iterator containing SinglePatternODAG stashes which hold
   * compressed embeddings
   * @return some embedding or none
   */
  def getNextInboundEmbedding(
      remainingStashes: Iterator[ODAGStash]): Option[O] = {
    if (currentEmbeddingStash == null) {
      
      if (remainingStashes.hasNext) {

        currentEmbeddingStash = remainingStashes.next
        currentEmbeddingStash.finalizeConstruction (pool,
          numPartitionsPerWorker)

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
   * Any Spark accumulator used for stats accounting is flushed here
   */
  private def flushStatsAccumulators: Unit = {
    // accumulates an aggregator in the corresponding spark accumulator
    def accumulate[T : ClassTag](it: T, accum: Accumulator[_]) = {
      accum.asInstanceOf[Accumulator[T]] += it
    }
    logInfo (s"Embeddings processed: ${numEmbeddingsProcessed}")
    accumulate (numEmbeddingsProcessed,
      accums(SparkODAGMasterEngine.AGG_EMBEDDINGS_PROCESSED))
    logInfo (s"Embeddings generated: ${numEmbeddingsGenerated}")
    accumulate (numEmbeddingsGenerated,
      accums(SparkODAGMasterEngine.AGG_EMBEDDINGS_GENERATED))
    logInfo (s"Embeddings output: ${numEmbeddingsOutput}")
    accumulate (numEmbeddingsOutput,
      accums(SparkODAGMasterEngine.AGG_EMBEDDINGS_OUTPUT))
  }

  /**
   * Flushes a given aggregation.
   *
   * @param name name of the aggregation
   * @return iterator of aggregation storages
   * TODO: split aggregations before flush them and review the return type
   */
  def flushAggregationsByName(name: String) = {
    // does the final local aggregation
    // e.g. for motifs, turns quick patterns into canonical ones
    def aggregate[K <: Writable, V <: Writable](agg1: AggregationStorage[K,V], agg2: AggregationStorage[_,_]) = {
      agg1.finalLocalAggregate (agg2.asInstanceOf[AggregationStorage[K,V]])
      agg1
    }
    val aggStorage = getAggregationStorage(name)
    val finalAggStorage = aggregate (
      aggregationStorageFactory.createAggregationStorage (name),
      aggStorage)
    Iterator(finalAggStorage)
  }

  def flush: Iterator[(_,_)] = configuration.
      getString ("flush_method", SparkConfiguration.FLUSH_BY_PATTERN) match {
    case SparkConfiguration.FLUSH_BY_PATTERN => flushByPattern
    case SparkConfiguration.FLUSH_BY_ENTRIES => flushByEntries
    case SparkConfiguration.FLUSH_BY_PARTS =>   flushByParts
  }

  /**
   * Naively flushes outbound odags.
   * We assume that this execEngine is ready to
   * do *aggregationFilter*, i.e., this execution engine was generated by
   * [[withNewAggregations]].
   *
   * @return iterator of pairs of (pattern, odag)
   */
  def flushByPattern: Iterator[(Pattern,SinglePatternODAG)]  = {
    // consume content in *nextEmbeddingStash*
    for (odag <- nextEmbeddingStash.getEzips().iterator
         if computation.aggregationFilter (odag.getPattern))
      yield (odag.getPattern(), odag)
  }

  /** 
   * Flushes outbound odags in parts, i.e., with single domain entries per odag
   * We assume that this execEngine is ready to
   * do *aggregationFilter*, i.e., this execution engine was generated by
   * [[withNewAggregations]].
   *
   *  @return iterator of pairs of ((pattern,domainId,wordId), odag_with_one_entry)
   */
  def flushByEntries: Iterator[((Pattern,Int,Int), SinglePatternODAG)] = {

    /**
     * Iterator that split a big SinglePatternODAG into small ODAGs containing only one entry
     * of the original. Thus, keyed by (pattern, domainId, wordId)
     */
    class ODAGPartsIterator(odag: SinglePatternODAG) extends Iterator[((Pattern,Int,Int),SinglePatternODAG)] {

      val domainIterator = odag.getStorage().getDomainEntries().iterator
      var domainId = -1
      var currEntriesIterator: Option[Iterator[(Integer,DomainEntry)]] = None

      val reusableOdag = new SinglePatternODAG(odag.getPattern(), odag.getNumberOfDomains())

      @scala.annotation.tailrec
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

      @scala.annotation.tailrec
      private def nextRec: ((Pattern,Int,Int),SinglePatternODAG) = currEntriesIterator match {

        case None => // set next domain and recursive call
          currEntriesIterator = Some(domainIterator.next.iterator)
          domainId += 1
          nextRec

        case Some(entriesIterator) => // format domain entry as new SinglePatternODAG
          val newOdag = new SinglePatternODAG(odag.getPattern(), odag.getNumberOfDomains())
          val (wordId, entry) = entriesIterator.next
          val domainEntries = newOdag.getStorage().getDomainEntries()

          domainEntries.get (domainId).put (wordId, entry)

          ((newOdag.getPattern(),domainId,wordId.intValue), newOdag)

      }

      override def next = nextRec
    }

    // filter and flush
    nextEmbeddingStash.getEzips.iterator.
      filter (odag => computation.aggregationFilter (odag.getPattern)).
      flatMap (new ODAGPartsIterator(_))
  }

  /**
   * Flushes outbound odags by chunks of bytes
   * We assume that this execEngine is ready to
   * do *aggregationFilter*, i.e., this execution engine was generated by
   * [[withNewAggregations]].
   *
   * @return iterator of pairs ((pattern,partId), bytes)
   */
  def flushByParts: Iterator[((Pattern,Int),Array[Byte])] = {

    val outputs = Array.fill[ByteArrayOutputStream](numPartitionsPerWorker)(new ByteArrayOutputStream())
    def createDataOutput(output: OutputStream): DataOutput = new DataOutputStream(output)
    val dataOutputs = outputs.map (output => createDataOutput(output))
    val hasContent = new Array[Boolean](numPartitionsPerWorker)

    val parts = ListBuffer.empty[((Pattern,Int),Array[Byte])]

    for (odag <- nextEmbeddingStash.getEzips().iterator
         if computation.aggregationFilter (odag.getPattern)) {
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
    numEmbeddingsGenerated += 1
  }

  /**
   * Returns the current value of an aggregation installed in this execution
   * engine.
   *
   * @param name name of the aggregation
   * @return the aggregated value or null if no aggregation was found
   */
  override def getAggregatedValue[A <: Writable](name: String): A =
    previousAggregationsBc.value.asInstanceOf[Map[String,A]].get(name) match {
      case Some(aggStorage) => aggStorage
      case None =>
        logWarning (s"Previous aggregation storage $name not found")
        null.asInstanceOf[A]
    }

  /**
   * Maps (key,value) to the respective local aggregator
   *
   * @param name identifies the aggregator
   * @param key key to account for
   * @param value value to be accounted for key in that aggregator
   * 
   */
  override def map[K <: Writable, V <: Writable](name: String, key: K, value: V) = {
    val aggStorage = getAggregationStorage[K,V] (name)
    aggStorage.aggregateWithReusables (key, value)
  }

  /**
   * Retrieves or creates the local aggregator for the specified name.
   * Obs. the name must match to the aggregator's metadata configured in
   * *initAggregations* (Computation)
   *
   * @param name aggregator's name
   * @return an aggregation storage with the specified name
   */
  private def getAggregationStorage[K <: Writable, V <: Writable](name: String)
      : AggregationStorage[K,V] = aggregationStorages.get(name) match {
    case Some(aggregationStorage : AggregationStorage[K,V]) => aggregationStorage
    case None =>
      val aggregationStorage = aggregationStorageFactory.createAggregationStorage (name)
      aggregationStorages.update (name, aggregationStorage)
      aggregationStorage.asInstanceOf[AggregationStorage[K,V]]
    case Some(aggregationStorage) =>
      val e = new RuntimeException (s"Unexpected type for aggregation ${aggregationStorage}")
      logError (s"Wrong type of aggregation storage: ${e.getMessage}")
      throw e
  }

  /**
   * TODO: change srialization ??
   */
  override def output(embedding: Embedding) = embeddingWriterOpt match {
    case Some(embeddingWriter) =>
      val resEmbedding = ResultEmbedding (embedding)
      embeddingWriter.append (NullWritable.get, resEmbedding)

    case None =>

      // we must decide at runtime the concrete Writable to be used
      val resEmbeddingClass = if (embedding.isInstanceOf[EdgeInducedEmbedding])
        classOf[EEmbedding]
      else if (embedding.isInstanceOf[VertexInducedEmbedding])
        classOf[VEmbedding]
      else
        classOf[ResultEmbedding] // not allowed, will crash and should not happen

      // instantiate the embedding writer (sequence file)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")
      val embeddingWriter = SequenceFile.createWriter(hadoopConf.value,
        SeqWriter.file(partitionPath),
        SeqWriter.keyClass(classOf[NullWritable]),
        SeqWriter.valueClass(resEmbeddingClass))

      embeddingWriterOpt = Some(embeddingWriter)
      
      val resEmbedding = ResultEmbedding (embedding)
      embeddingWriter.append (NullWritable.get, resEmbedding)
  }

  /**
   * Maybe output string to fileSystem
   *
   * @param outputString data to write
   */
  override def output(outputString: String) = {
    if (configuration.isOutputActive) {
      writeOutput(outputString)
      numEmbeddingsOutput += 1
    }
  }

  private def writeOutput(outputString: String) = outputStreamOpt match {
    case Some(outputStream) =>
      outputStream.write(outputString)
      outputStream.write("\n")

    case None =>
      logInfo (s"[partitionId=${getPartitionId}] Creating output stream")
      val fs = FileSystem.get(hadoopConf.value)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")
      val outputStream = new OutputStreamWriter(fs.create(partitionPath))
      outputStreamOpt = Some(outputStream)
      outputStream.write(outputString)
      outputStream.write("\n")
  }
  
  // other functions
  override def getPartitionId() = partitionId

  override def getNumberPartitions() = configuration.getInteger ("num_partitions", 10)

  override def getSuperstep() = superstep

  override def aggregate(name: String, value: LongWritable) = accums.get (name) match {
    case Some(accum) =>
      accum.asInstanceOf[Accumulator[Long]] += value.get
    case None => 
      logWarning (s"Aggregator/Accumulator $name not found")
  }
}
