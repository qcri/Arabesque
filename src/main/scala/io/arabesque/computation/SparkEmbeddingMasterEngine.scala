package io.arabesque.computation

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.cache.LZ4ObjectCache
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.odag.ODAG
import io.arabesque.utils.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Accumulator, HashPartitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkEmbeddingMasterEngine[E <: Embedding]
    (config: SparkConfiguration[E]) extends SparkMasterEngine(config) {

  import SparkEmbeddingMasterEngine._

  // testing
  config.initialize()

  // Spark accumulators for stats counting (non-critical)
  // Ad-hoc arabesque approach for user-defined aggregations
  private var aggAccums: Map[String,Accumulator[_]] = _
  private var aggregations
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  private var superstep = 0

  private var masterComputation: MasterComputation = _

  def this(_sc: SparkContext, config: SparkConfiguration[E]) {
    this (config)
    sc = _sc
    init()
  }

  def sparkContext: SparkContext = sc
  def arabConfig: SparkConfiguration[_ <: Embedding] = config

  override def init() = {

    val logLevel = Level.toLevel (config.getLogLevel)
    Logger.getLogger(logName).setLevel (logLevel)

    // garantees that outputPath does not exist
    if (config.isOutputActive) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val outputPath = new Path(config.getOutputPath)
      if (fs.exists (outputPath))
        throw new RuntimeException (
          s"Output path ${config.getOutputPath} exists. Choose another one."
          )
    }
    
    // master computation
    masterComputation = config.createMasterComputation()
    masterComputation.setUnderlyingExecutionEngine(this)
    masterComputation.init()

    // master must know aggregators metadata
    val computation = config.createComputation()
    computation.initAggregations()

    // stats aggregation via accumulators
    aggAccums = Map.empty
    aggAccums.update (AGG_EMBEDDINGS_GENERATED,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_GENERATED))
    aggAccums.update (AGG_EMBEDDINGS_PROCESSED,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_PROCESSED))
    aggAccums.update (AGG_EMBEDDINGS_OUTPUT,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_OUTPUT))

  }

  override def haltComputation() = {
    logInfo ("Halting master computation")
    sc.stop()
  }

  override def getSuperstep(): Long = superstep

  /**
   * Master's computation takes place here, superstep by superstep
   */
  override def compute() = {
    val numPartitions = config.getInteger ("num_partitions", 10)

    // accumulatores and spark configuration w.r.t. Spark
    // TODO: ship serHaddopConf with SparkConfiguration
    val configBc = sc.broadcast(config)
    val serHadoopConf = new SerializableConfiguration(sc.hadoopConfiguration)

    // superstepRDD in this engine represents an RDD of compressed caches, which
    // contain embeddings from the previous superstep.
    var superstepRDD = sc.makeRDD (Seq.empty[LZ4ObjectCache], numPartitions)
    // RDD of execution engines
    var execEngines = sc.makeRDD (Seq.empty[SparkEmbeddingEngine[E]], numPartitions).cache

    var previousAggregationsBc: Broadcast[_] = sc.broadcast (
      Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]
    )

    val startTime = System.currentTimeMillis

    do {

      val _aggAccums = aggAccums
      val superstepStart = System.currentTimeMillis

      // save old execution engines for unpersisting
      val _execEngines = execEngines
      execEngines = getExecutionEngines (
        superstepRDD = superstepRDD,
        superstep = superstep,
        configBc = configBc,
        serHadoopConf = serHadoopConf,
        aggAccums = _aggAccums,
        previousAggregationsBc = previousAggregationsBc)

      // keep engines (filled with expansions and aggregations) for the rest of
      // the superstep
      execEngines.persist (MEMORY_AND_DISK_SER)

      /** [1] We extract and aggregate the *aggregations* globally.
       */

      val aggregationsFuture = getAggregations (execEngines, numPartitions)
      // aggregations
      Await.ready (aggregationsFuture, atMost = Duration.Inf)
      aggregationsFuture.value.get match {
        case Success(previousAggregations) =>

          aggregations = mergeOrReplaceAggregations (aggregations, previousAggregations)
          
          logInfo (s"""Aggregations and sizes
            ${aggregations.
            map(tup => (tup._1,tup._2.getNumberMappings)).mkString("\n")}
          """)

          previousAggregationsBc.unpersist()
          previousAggregationsBc = sc.broadcast (aggregations)

        case Failure(e) =>
          logError (s"Error in collecting aggregations: ${e.getMessage}")
          throw e
      }

      // barrier: get rid of the old engines
      _execEngines.unpersist()

      /** [2] We shuffle the embeddings and prepare to the next superstep
       */
      superstepRDD = execEngines.
        flatMap (_.flush).
        partitionBy (new HashPartitioner (numPartitions)).
        values
      
      // whether the user chose to customize master computation, executed every
      // superstep
      masterComputation.compute()

      val superstepFinish = System.currentTimeMillis
      logInfo (s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms")
      
      // print stats
      aggAccums = aggAccums.map { case (name,accum) =>
        logInfo (s"Accumulator[$name]: ${accum.value}")
        (name -> sc.accumulator [Long] (0L, name))
      }
      
      superstep += 1

      // while there are embeddings to be processed
    } while (!sc.isStopped && !superstepRDD.isEmpty)

    val finishTime = System.currentTimeMillis

    logInfo (s"Computation has finished. It took ${finishTime - startTime} ms")
    
  }

  /**
   * Creates an RDD of execution engines 
   * TODO
   */
  private def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[LZ4ObjectCache],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      serHadoopConf: SerializableConfiguration,
      aggAccums: Map[String,Accumulator[_]],
      previousAggregationsBc: Broadcast[_]) = {

    // read embeddings from embedding caches, expand, filter and process
    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, cacheIter) =>

      configBc.value.initialize()

      val execEngine = new SparkEmbeddingEngine [E] (
        partitionId = idx,
        superstep = superstep,
        hadoopConf = serHadoopConf,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc
      )

      execEngine.init()
      execEngine.compute (cacheIter)
      execEngine.finalize()
      Iterator(execEngine)
    }

    execEngines
  }

  /**
   * Extracts and aggregate AggregationStorages from executionEngines
   *
   * @param execEngines rdd of spark execution engines
   * @param numPartitions based on the number of partitions, we decide the
   * depth of the aggregation tree
   *
   * @return a future with a map (name -> aggregationStorage) as entries
   *
   */
  def getAggregations(
      execEngines: RDD[SparkEmbeddingEngine[E]],
      numPartitions: Int) = Future {

    def reduce[K <: Writable, V <: Writable](
        name: String,
        metadata: AggregationStorageMetadata[K,V])
      (implicit kt: ClassTag[K], vt: ClassTag[V]) =
        Future[AggregationStorage[_ <: Writable, _ <: Writable]] {

      val keyValues = execEngines.flatMap (execEngine =>
          execEngine.flushAggregationsByName(name).
            asInstanceOf[Iterator[AggregationStorage[K,V]]]
          )
      val aggStorage = keyValues.reduce { (agg1,agg2) =>
        agg1.aggregate (agg2)
        agg1
      }
      
      aggStorage.endedAggregation
      aggStorage
    }

    val future = Future.sequence (
      config.getAggregationsMetadata.map { case (name, metadata) =>
        reduce (name, metadata)
      }
    )

    val aggregations = Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]

    Await.ready (future, Duration.Inf)
    future.value.get match {
      case Success(aggStorages) =>
        aggStorages.foreach (aggStorage => aggregations.update (aggStorage.getName, aggStorage))
      case Failure(e) =>
        throw e
    }

    aggregations
  }

  override def getAggregatedValue[T <: Writable](name: String) = aggregations.get(name) match {
    case Some(aggStorage) => aggStorage.asInstanceOf[T]
    case None =>
      logWarning (s"AggregationStorage $name not found")
      null.asInstanceOf[T]
  }

  override def setAggregatedValue[T <: Writable](name: String, value: T) = {
    logWarning ("Setting aggregated value has no effect in spark execution engine")
  }

  override def finalizeComputation() = {
    super.finalize()
  }
}

/**
 * Companion object: static methods and fields
 */
object SparkEmbeddingMasterEngine {
  val AGG_EMBEDDINGS_PROCESSED = "embeddings_processed"
  val AGG_EMBEDDINGS_GENERATED = "embeddings_generated"
  val AGG_EMBEDDINGS_OUTPUT = "embeddings_output"
}
