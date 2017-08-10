package io.arabesque.compression

import java.io._
import java.util.concurrent.{ExecutorService, Executors}

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageFactory}
import io.arabesque.computation._
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._
import io.arabesque.report._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, SequenceFile, Writable}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.Map
import scala.reflect.ClassTag

/**
  * Created by ehussein on 7/9/17.
  */
trait SimpleStorageEngine [
    E <: Embedding,
    Storage <: SimpleStorage,
    Stash <: SimpleStorageStash[Storage,Stash],
    C <: SimpleStorageEngine[E,Storage,Stash,C]
  ]
  extends SparkEngine[E] {

  // superstep arguments
  val partitionId: Int
  val superstep: Int
  val accums: Map[String,Accumulator[_]]
  val previousAggregationsBc: Broadcast[_]

  //val report: PartitionReport = new PartitionReport
  //val reportsFilePath: String = "/home/ehussein/Downloads/ArabesqueTesting/compresssion/reports/"

  // update aggregations before flush
  def withNewAggregations(aggregationsBc: Broadcast[_]): C

  // flush odags
  def flush: Iterator[(_,_)]

  // stashes: it is dependent of odag and stash implementation
  //var previousEmbeddingStash: Stash = _
  var currentEmbeddingStashOpt: Option[Stash] = None
  var nextEmbeddingStash: Stash = _
  @transient var stashReader: EfficientReader[E] = _
  //@transient var stashReader: SimpleStorageStash[Storage,Stash]#EfficientReader[E] = _

  @transient lazy val computation: Computation[E] = {
    val computation = configuration.createComputation [E]
    computation.setUnderlyingExecutionEngine (this)
    computation.init()
    computation.initAggregations()
    computation
  }

  // reader parameters
  lazy val numBlocks: Int =
    configuration.getInteger ("numBlocks", getNumberPartitions() * getNumberPartitions())
  lazy val maxBlockSize: Int =
    configuration.getInteger ("maxBlockSize", 10000) // TODO: magic number ??

  lazy val numPartitionsPerWorker = configuration.numPartitionsPerWorker

  // aggregation storages
  @transient lazy val aggregationStorageFactory = new AggregationStorageFactory
  lazy val aggregationStorages
  : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = Map.empty

  // accumulators
  var numEmbeddingsProcessed: Long = 0
  var numEmbeddingsGenerated: Long = 0
  var numEmbeddingsOutput: Long = 0
  var numSpuriousEmbeddings: Long = 0

  // TODO: tirar isso !!!
  def init(): Unit = {
    /*
    report.partitionId = this.partitionId
    report.superstep = this.superstep
    report.startTime = System.currentTimeMillis()
    */
  }

  // output
  @transient var embeddingWriterOpt: Option[SeqWriter] = None
  @transient var outputStreamOpt: Option[OutputStreamWriter] = None
  @transient lazy val outputPath: Path = new Path(configuration.getOutputPath)

  /**
    * Releases resources allocated for this instance
    */
  override def finalize() = {
    super.finalize()
    // make sure we close writers
    if (outputStreamOpt.isDefined) outputStreamOpt.get.close
    if (embeddingWriterOpt.isDefined) embeddingWriterOpt.get.close

    // set the finish time for this partition computation
    /*
    report.endTime = System.currentTimeMillis()
    report.saveReport(reportsFilePath)
    */
  }

  /**
    * It does the computation of this module, i.e., expand/compute
    *
    * @param inboundStashes iterator of SimpleStorage stashes
    */
  def compute(inboundStashes: Iterator[Stash]) = {
    logInfo (s"Computing partition(${partitionId}) of superstep ${superstep}")
    if (computed)
      throw new RuntimeException ("computation must be atomic")

    if (configuration.getEmbeddingClass() == null)
      configuration.setEmbeddingClass (computation.getEmbeddingClass())

    //val stash = inboundStashes.to[SinglePatternSimpleStorageStash]
    expansionCompute (inboundStashes)
    flushStatsAccumulators
    computed = true
  }

  /**
    * Iterates over SimpleStorage stashes and call expansion/compute procedures on them.
    * It also bootstraps the cycle by requesting empty embedding from
    * configuration and expanding them.
    *
    * @param inboundStashes iterator of SimpleStorage stashes
    */
  private def expansionCompute(inboundStashes: Iterator[Stash]): Unit = {
    if (superstep == 0) { // bootstrap

      val initialEmbedd: E = configuration.createEmbedding()
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
  def internalCompute(embedding: E) = computation.expand (embedding)

  /**
    * Reads next embedding from previous Storage
    *
    * @param remainingStashes iterator containing SinglePatternSimpleStorage stashes which hold
    * compressed embeddings
    * @return some embedding or none
    */
  def getNextInboundEmbedding(remainingStashes: Iterator[Stash]): Option[E] = {
    if (!currentEmbeddingStashOpt.isDefined) {
      // this if statement will be executed once we have finished reading a stash
      /*
      if(previousEmbeddingStash != null) {
        previousEmbeddingStash.saveStorageReports(reportsFilePath+s"partition${partitionId}_superstep_${superstep}_report.txt")

        // calc spurious
        /*
        val currentSpurious = previousEmbeddingStash.getNumberSpuriousEmbeddings
        println(s"PartitionId=$partitionId, SpuriousEmbeddings(CurrentStash)=${currentSpurious}")
        // accumulate to the partition' global accumulator
        numSpuriousEmbeddings += currentSpurious
        */
      }
      */

      if (remainingStashes.hasNext) {

        val currentEmbeddingStash = remainingStashes.next

        //previousEmbeddingStash = currentEmbeddingStash

        currentEmbeddingStash.finalizeConstruction (
          SimpleStorageEngine.pool(numPartitionsPerWorker),
          numPartitionsPerWorker)

        // simple_storage stashes have an efficient reader for compressed embeddings
        stashReader = new (EfficientReader[E])  (currentEmbeddingStash,
          computation,
          getNumberPartitions(),
          numBlocks,
          maxBlockSize)

        currentEmbeddingStashOpt = Some(currentEmbeddingStash)

      }
      else
        return None
    }

    // new embedding was found
    if (stashReader.hasNext) {
      Some(stashReader.next)
      // no more embeddings to be read from current stash, try to get another
      // stash by recursive call
    } else {
      currentEmbeddingStashOpt = None
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
    logInfo (s"Embeddings processed: ${numEmbeddingsProcessed} by partition($partitionId) in SuperStep($superstep)")
    accumulate (numEmbeddingsProcessed,
      accums(SimpleStorageMasterEngine.AGG_EMBEDDINGS_PROCESSED))

    logInfo (s"Embeddings generated: ${numEmbeddingsGenerated} by partition($partitionId) in SuperStep($superstep)")
    accumulate (numEmbeddingsGenerated,
      accums(SimpleStorageMasterEngine.AGG_EMBEDDINGS_GENERATED))

    logInfo (s"Embeddings output: ${numEmbeddingsOutput} by partition($partitionId) in SuperStep($superstep)")
    accumulate (numEmbeddingsOutput,
      accums(SimpleStorageMasterEngine.AGG_EMBEDDINGS_OUTPUT))

    logInfo (s"Spurious Embeddings: ${numSpuriousEmbeddings} by partition($partitionId) in SuperStep($superstep)")
    accumulate (numSpuriousEmbeddings,
      accums(SimpleStorageMasterEngine.AGG_SPURIOUS_EMBEDDINGS))
  }

  /**
    * Flushes a given aggregation.
    *
    * @param name name of the aggregation
    * @return iterator of aggregation storages
    * TODO: split aggregations before flush them and review the return type
    */
  def flushAggregationsByName(name: String) = {
    // the following function does the final local aggregation
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
  def addOutboundEmbedding(embedding: E) = processExpansion (embedding)

  /**
    * Adds an expansion (embedding) to the outbound odags.
    *
    * @param expansion embedding to be added to the stash of outbound odags
    */
  override def processExpansion(expansion: E) = {
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
  override def getAggregationStorage[K <: Writable, V <: Writable](name: String)
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
    * TODO: change serialization ??
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

object SimpleStorageEngine {
  import Configuration._
  import SparkConfiguration._

  def apply [E <: Embedding, O <: SimpleStorage, S <: SimpleStorageStash[O,S], C <: SimpleStorageEngine[E,O,S,C]]
  (config: Configuration[E], partitionId: Int, superstep: Int, accums: Map[String,Accumulator[_]], previousAggregationsBc: Broadcast[_])
  : C = config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT) match {
      case COMM_SIMPLE_STORAGE_SP =>
        new SimpleStorageEngineSP [E] (partitionId, superstep, accums, previousAggregationsBc).asInstanceOf[C]
    }

  // pool related vals
  private var poolOpt: Option[ExecutorService] = None

  def pool(poolSize: Int) = poolOpt match {
    case Some(pool) => pool
    case None =>
      val pool = Executors.newFixedThreadPool (poolSize)
      poolOpt = Some(pool)
      pool
  }

  def shutdownPool: Unit = poolOpt match {
    case Some(pool) => pool.shutdown()
    case None =>
  }

  override def finalize: Unit = shutdownPool
}
