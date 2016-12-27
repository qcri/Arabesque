package io.arabesque.computation

import java.io.OutputStreamWriter

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.Configuration
import io.arabesque.embedding._
import io.arabesque.utils.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Writable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
 * Spark engine that works with raw embedding representation
 */
case class SparkEmbeddingEngine[O <: Embedding](
    partitionId: Int,
    superstep: Int,
    accums: Map[String,Accumulator[_]],
    // TODO do not broadcast if user's code does not requires it
    previousAggregationsBc: Broadcast[_])
  extends SparkEngine[O] {

  @transient lazy val computation: Computation[O] = {
    val computation = configuration.createComputation [O]
    computation.setUnderlyingExecutionEngine (this)
    computation.init()
    computation.initAggregations()
    computation
  }

  // embedding caches
  var embeddingCaches: Array[LZ4ObjectCache] = _
  var currentCache: LZ4ObjectCache = _

  // round robin id
  var _nextGlobalId: Long = _
  def nextGlobalId: Long = {
    val id = _nextGlobalId
    _nextGlobalId += 1
    id
  }

  // aggregation storages
  @transient lazy val aggregationStorageFactory = new AggregationStorageFactory
  var aggregationStorages
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

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
    if (configuration.getEmbeddingClass() == null)
      configuration.setEmbeddingClass (computation.getEmbeddingClass())

    // embedding caches
    embeddingCaches = Array.fill (getNumberPartitions) (new LZ4ObjectCache)
    currentCache = null

    // global round-robin id
    val countersPerPartition = Long.MaxValue / getNumberPartitions
    _nextGlobalId = countersPerPartition * partitionId

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
  }

  /**
   * It does the computation of this module, i.e., expand/compute
   *
   * @param inboundCaches
   */
  def compute(inboundCaches: Iterator[LZ4ObjectCache]) = {
    expansionCompute (inboundCaches)
    flushStatsAccumulators
  }

  /**
   * Iterates over embedding caches and call expansion/compute procedures on them.
   * It also bootstraps the cycle by requesting empty embedding from
   * configuration and expanding them.
   *
   * @param inboundCaches iterator of embedding caches
   */
  private def expansionCompute(inboundCaches: Iterator[LZ4ObjectCache]): Unit = {
    if (superstep == 0) { // bootstrap

      val initialEmbedd: O = configuration.createEmbedding()
      computation.expand (initialEmbedd)

    } else {
      var hasNext = true
      while (hasNext) getNextInboundEmbedding (inboundCaches) match {
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
   * Reads next embedding from previous caches
   *
   * @param remainingCaches 
   * @return some embedding or none
   */
  @scala.annotation.tailrec
  private def getNextInboundEmbedding(
      remainingCaches: Iterator[LZ4ObjectCache]): Option[O] = {
    if (currentCache == null) {
      if (remainingCaches.hasNext) {
        currentCache = remainingCaches.next
        currentCache.prepareForIteration
        getNextInboundEmbedding (remainingCaches)
      } else None
    } else {
      if (currentCache.hasNext) {
        val embedding = currentCache.next.asInstanceOf[O]
        if (computation.aggregationFilter(embedding.getPattern))
          Some(embedding)
        else
          getNextInboundEmbedding (remainingCaches)
      } else {
        currentCache = null
        getNextInboundEmbedding (remainingCaches)
      }
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
      accums(ODAGMasterEngine.AGG_EMBEDDINGS_PROCESSED))
    logInfo (s"Embeddings generated: ${numEmbeddingsGenerated}")
    accumulate (numEmbeddingsGenerated,
      accums(ODAGMasterEngine.AGG_EMBEDDINGS_GENERATED))
    logInfo (s"Embeddings output: ${numEmbeddingsOutput}")
    accumulate (numEmbeddingsOutput,
      accums(ODAGMasterEngine.AGG_EMBEDDINGS_OUTPUT))
  }

  /**
   * Flushes a given aggregation.
   *
   * @param name name of the aggregation
   *
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
    val destId = (nextGlobalId % getNumberPartitions).toInt
    val cache = embeddingCaches(destId)
    cache.addObject (expansion)
    numEmbeddingsGenerated += 1
  }

  def flush: Iterator[(Int,LZ4ObjectCache)] = {
    if (numEmbeddingsGenerated > 0)
      (0 until embeddingCaches.size).iterator.
        map (i => (i, embeddingCaches(i)))
    else Iterator.empty
  }

  /**
   * Returns the current value of an aggregation installed in this execution
   * engine.
   *
   * @param name name of the aggregation
   *
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
      numEmbeddingsOutput += 1

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
      val embeddingWriter = SequenceFile.createWriter(configuration.hadoopConf,
        SeqWriter.file(partitionPath),
        SeqWriter.keyClass(classOf[NullWritable]),
        SeqWriter.valueClass(resEmbeddingClass))

      embeddingWriterOpt = Some(embeddingWriter)
      
      val resEmbedding = ResultEmbedding (embedding)
      embeddingWriter.append (NullWritable.get, resEmbedding)
      numEmbeddingsOutput += 1
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
      val fs = FileSystem.get(configuration.hadoopConf)
      val superstepPath = new Path(outputPath, s"${getSuperstep}")
      val partitionPath = new Path(superstepPath, s"${partitionId}")
      val outputStream = new OutputStreamWriter(fs.create(partitionPath))
      outputStreamOpt = Some(outputStream)
      outputStream.write(outputString)
      outputStream.write("\n")
  }
  
  // other functions
  override def getPartitionId() = partitionId

  override def getSuperstep() = superstep

  override def aggregate(name: String, value: LongWritable) = accums.get (name) match {
    case Some(accum) =>
      accum.asInstanceOf[Accumulator[Long]] += value.get
    case None => 
      logWarning (s"Aggregator/Accumulator $name not found")
  }
}
