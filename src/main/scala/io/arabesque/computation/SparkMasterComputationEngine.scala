package io.arabesque.computation

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, Accumulable}
import org.apache.spark.{AccumulatorParam, AccumulableParam}
import org.apache.spark.rdd.RDD

import org.apache.spark.util.SizeEstimator

import org.apache.hadoop.io.{Writable, LongWritable, IntWritable, NullWritable}
import org.apache.hadoop.fs.{Path, FileSystem}

import io.arabesque.graph.BasicMainGraph

import io.arabesque.aggregation.{AggregationStorage,
                                 PatternAggregationStorage,
                                 AggregationStorageMetadata,
                                 AggregationStorageFactory}

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.conf.Configuration._
import io.arabesque.odag.{ODAG, ODAGStash}

import io.arabesque.embedding.{Embedding, VertexInducedEmbedding, EdgeInducedEmbedding,
                               ResultEmbedding, VEmbedding, EEmbedding}

import io.arabesque.pattern.Pattern
import io.arabesque.utils.{SerializableConfiguration, SerializableWritable}

import scala.collection.mutable.Map
import scala.collection.JavaConversions._
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

import java.util.concurrent.{ExecutorService, Executors}
import java.io.{DataOutput, ByteArrayOutputStream, DataOutputStream, OutputStream,
                DataInput, ByteArrayInputStream, DataInputStream, InputStream}

import scala.reflect.ClassTag

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkMasterExecutionEngine(config: SparkConfiguration[_ <: Embedding]) extends
    CommonMasterExecutionEngine with Logging {

  import SparkMasterExecutionEngine._

  // testing
  config.initialize()

  private var sc: SparkContext = _

  // Spark accumulators for stats counting (non-critical)
  // Ad-hoc arabesque approach for user-defined aggregations
  private var aggAccums: Map[String,Accumulator[_]] = _
  private var aggregations
    : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  private var superstep = 0

  private var masterComputation: MasterComputation = _

  private var odags: List[RDD[ODAG]] = List()
  private var embeddings: List[RDD[Array[Int]]] = List()

  def this(confs: Map[String,Any]) {
    this (new SparkConfiguration(confs))

    sc = new SparkContext(config.sparkConf)
    val logLevel = config.getString ("log_level", "INFO").toUpperCase
    sc.setLogLevel (logLevel)

    init()
  }

  def this(_sc: SparkContext, config: SparkConfiguration[_ <: Embedding]) {
    this (config)
    sc = _sc
    init()
  }

  /** user must call init() after */
  def this(_sc: SparkContext) {
    this (Map.empty[String,Any])
  }

  def sparkContext: SparkContext = sc
  def arabConfig: SparkConfiguration[_ <: Embedding] = config

  def init() = {

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
  def compute() = {
    val numPartitions = config.getInteger ("num_partitions", 10)

    // accumulatores and spark configuration w.r.t. Spark
    // TODO: ship serHaddopConf with SparkConfiguration
    val configBc = sc.broadcast(config)
    val serHadoopConf = new SerializableConfiguration(sc.hadoopConfiguration)

    // setup an RDD to simulate empty partitions and a broadcast variable to
    // communicate the global aggregated ODAGs on each step
    val superstepRDD = sc.makeRDD (Seq.empty[Any], numPartitions).cache
    var aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,ODAG]] =
      sc.broadcast (Map.empty)

    var previousAggregationsBc: Broadcast[_] = sc.broadcast (
      Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]
    )

    val startTime = System.currentTimeMillis

    do {

      val _aggAccums = aggAccums
      val superstepStart = System.currentTimeMillis

      val execEngines = getExecutionEngines (
        superstepRDD = superstepRDD,
        superstep = superstep,
        configBc = configBc,
        aggregatedOdagsBc = aggregatedOdagsBc,
        serHadoopConf = serHadoopConf,
        aggAccums = _aggAccums,
        previousAggregationsBc = previousAggregationsBc)

      // keep engines (filled with expansions and aggregations) for the rest of
      // the superstep
      execEngines.cache

      /** [1] We extract and aggregate the *aggregations* globally.
       *  That gives us the opportunity to do aggregationFilter in the generated
       *  ODAGs before collecting/broadcasting */

      // create futures (two jobs submitted roughly simultaneously)
      val aggregationsFuture = getAggregations (execEngines, numPartitions)
      // aggregations
      Await.ready (aggregationsFuture, atMost = Duration.Inf)
      aggregationsFuture.value.get match {
        case Success(previousAggregations) =>

          aggregations = previousAggregations
          
          logInfo (s"""Aggregations and sizes
            ${previousAggregations.
            map(tup => (tup._1,tup._2.getNumberMappings)).mkString("\n")}
          """)

          previousAggregationsBc.unpersist()
          previousAggregationsBc = sc.broadcast (previousAggregations)

        case Failure(e) =>
          logError (s"Error in collecting aggregations: ${e.getMessage}")
          throw e
      }

      /** [2] At this point we have updated the *previousAggregations*. Now we
       *  can: (i) aggregationFilter the ODAGs residing in the execution
       *  engines, if this applies; and (ii) flush the remaining ODAGs for
       *  global aggregation.
       */
      
      // we choose the flush method for ODAGs: load-balancing vs. overhead
      val aggregatedOdags = config.getString ("flush_method", SparkConfiguration.FLUSH_BY_PATTERN) match {
        case SparkConfiguration.FLUSH_BY_PATTERN =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flushByPattern)
          aggregatedOdagsByPattern (odags)

        case SparkConfiguration.FLUSH_BY_ENTRIES =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flushByEntries)
          aggregatedOdagsByEntries (odags)

        case SparkConfiguration.FLUSH_BY_PARTS =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flushByParts)
          aggregatedOdagsByParts (odags)
      }

      odags = aggregatedOdags.values :: odags

      val odagsFuture = Future { aggregatedOdags.collectAsMap  }
      // odags
      Await.ready (odagsFuture, atMost = Duration.Inf)
      odagsFuture.value.get match {
        case Success(aggregatedOdagsLocal) =>
          logInfo (s"Number of aggregated ODAGs = ${aggregatedOdagsLocal.size}")
          aggregatedOdagsBc.unpersist()
          aggregatedOdagsBc = sc.broadcast (aggregatedOdagsLocal)

        case Failure(e) =>
          logError (s"Error in collecting odags ${e.getMessage}")
          throw e
      }

      // the exec engines have no use anymore, make room for the next round
      execEngines.unpersist()
      
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

    } while (!sc.isStopped && !aggregatedOdagsBc.value.isEmpty) // while there are ODAGs to be processed

    val finishTime = System.currentTimeMillis

    logInfo (s"Computation has finished. It took ${finishTime - startTime} ms")
    
  }

  /**
   * Creates an RDD of execution engines 
   * TODO
   */
  private def getExecutionEngines[E <: Embedding](
      superstepRDD: RDD[Any],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,ODAG]],
      serHadoopConf: SerializableConfiguration,
      aggAccums: Map[String,Accumulator[_]],
      previousAggregationsBc: Broadcast[_]) = {

    // read embeddings from global agg. ODAGs, expand, filter and process
    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, _) =>

      configBc.value.initialize()

      val execEngine = new SparkExecutionEngine(
        partitionId = idx,
        superstep = superstep,
        hadoopConf = serHadoopConf,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc
      )
      execEngine.init()
      execEngine.compute (Iterator (new ODAGStash(aggregatedOdagsBc.value)))
      execEngine.finalize()
      Iterator(execEngine)
    }

    execEngines
  }

  private def aggregatedOdagsByPattern(odags: RDD[(Pattern, ODAG)]) = {

    // (flushByPattern)
    val aggregatedOdags = odags.reduceByKey { (odag1, odag2) =>
      odag1.aggregate (odag2)
      odag1
    }.
    map { case (pattern, odag) =>
      odag.setSerializeAsReadOnly (true)
      (pattern, odag)
    }

    aggregatedOdags
  }


  private def aggregatedOdagsByEntries(odags: RDD[((Pattern,Int,Int), ODAG)]) = {

    // (flushInEntries) ODAGs' reduction by pattern as a key
    val aggregatedOdags = odags.reduceByKey { (odag1, odag2) =>
      odag1.aggregate (odag2)
      odag1
      // resulting ODAGs must be deserialized for read(only)
    }.
    map { case ((pattern,_,_), odag) =>
      (pattern, odag)
      }.reduceByKey { (odag1, odag2) =>
        odag1.aggregate (odag2)
        odag1
      }.
    map { case (pattern,odag) =>
      odag.setSerializeAsReadOnly(true)
      (pattern,odag)
    }

    aggregatedOdags
  }
    

  private def aggregatedOdagsByParts(odags: RDD[((Pattern,Int), Array[Byte])]) = {

    // (flushByParts)
    val aggregatedOdags = odags.combineByKey (
      (byteArray: Array[Byte]) => {
        val dataInput = new DataInputStream(new ByteArrayInputStream(byteArray))
        val _odag = new ODAG(false)
        _odag.readFields (dataInput)
        _odag
      },
      (odag: ODAG, byteArray: Array[Byte]) => {
        val dataInput = new DataInputStream(new ByteArrayInputStream(byteArray))
        val _odag = new ODAG(false)
        _odag.readFields (dataInput)
        odag.aggregate (_odag)
        odag
      },
      (odag1: ODAG, odag2: ODAG) => {
        odag1.aggregate (odag2)
        odag1
      }
    ).
    map { case ((pattern,_),odag) =>
      (pattern,odag)
    }.reduceByKey { (odag1,odag2) =>
      odag1.aggregate (odag2)
      odag1
    }.
    map { tup =>
      tup._2.setSerializeAsReadOnly (true)
      tup
    }

    aggregatedOdags
  }

  /**
   * Extracts and aggregate AggregationStorages from executionEngines
   *
   * @param execEngines rdd of spark execution engines
   * @param numPartitions based on the number of partitions, we decide the
   * depth of the aggregation tree
   * @return a future with a map (name -> aggregationStorage) as entries
   *
   */
  def getAggregations(
      execEngines: RDD[SparkExecutionEngine[Nothing]],
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

  override def finalize() = {
    super.finalize()
  }

  /**
   * Functions that retrieve the results of this computation.
   * Current fields:
   *  - Odags of each superstep.
   *  - Embeddings if the output is enabled. Our choice is to read the results
   *  produced by the supersteps from external storage. We avoid memory issues
   *  by not keeping all the embeddings in memory.
   */
  def getOdags: RDD[ODAG] = {
    sc.union (odags.toSeq)
  }
  def getEmbeddings: RDD[ResultEmbedding] = {

    val embeddPath = s"${config.getOutputPath}"
    val fs = FileSystem.get (sc.hadoopConfiguration)

    if (config.isOutputActive && fs.exists (new Path (embeddPath))) {
      logInfo (s"Reading embedding words from: ${config.getOutputPath}")
      //sc.textFile (s"${embeddPath}/*").map (ResultEmbedding(_))

      // we must decide at runtime the concrete Writable to be used
      val resEmbeddingClass = if (config.getEmbeddingClass == classOf[EdgeInducedEmbedding])
        classOf[EEmbedding]
      else if (config.getEmbeddingClass == classOf[VertexInducedEmbedding])
        classOf[VEmbedding]
      else
        classOf[ResultEmbedding] // not allowed, will crash and should not happen

      sc.sequenceFile (s"${embeddPath}/*", classOf[NullWritable], resEmbeddingClass).
        values.
        asInstanceOf[RDD[ResultEmbedding]]
    } else {
      sc.emptyRDD[ResultEmbedding]
    }

  }
}

/**
 * Companion object: static methods and fields
 */
object SparkMasterExecutionEngine {
  val AGG_EMBEDDINGS_PROCESSED = "embeddings_processed"
  val AGG_PROCESSED_SIZE_ODAG = "processed_size_odag"
  val AGG_EMBEDDINGS_GENERATED = "embeddings_generated"
  val AGG_EMBEDDINGS_OUTPUT = "embeddings_output"
}

/**
 * Param used to accumulate Aggregation Storages
 */
class AggregationStorageParam[K <: Writable, V <: Writable](name: String) extends AccumulatorParam[AggregationStorage[K,V]] {
  def zero(initialValue: AggregationStorage[K,V]) = new AggregationStorage[K,V](name)
  def addInPlace(ag1: AggregationStorage[K,V], ag2: AggregationStorage[K,V]) = {
    ag1.aggregate (ag2)
    ag1
  }
}
