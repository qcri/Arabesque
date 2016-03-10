/* ArabesqueTest.scala */

package io.arabesque.computation

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, Accumulable}
import org.apache.spark.{AccumulatorParam, AccumulableParam}
import org.apache.spark.rdd.RDD

import org.apache.spark.util.SizeEstimator

import org.apache.hadoop.io.{Writable, LongWritable, IntWritable}

import io.arabesque.graph.BasicMainGraph

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.conf.Configuration._
import io.arabesque.odag.{ODAG, ODAGStash}
import io.arabesque.embedding.{Embedding, VertexInducedEmbedding}
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

  // TODO would be interesting to make Kryo the default serializer for Spark
  // however it would require great changes in the way classes are serialized by
  // default (Serializable vs. Externalizable vs. Writable for Hadoop)
  //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf.registerKryoClasses(Array(
  //  classOf[ODAG]
  //  ))
  
  //private var config: SparkConfiguration[_ <: Embedding] = _
  config.initialize()

  private var sc: SparkContext = _

  // TODO accumulators will not be used anymore for custom aggregation
  private var aggAccums: Map[String,Accumulator[_]] = _
  private var aggregations: Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  private var superstep = 0

  private var masterComputation: MasterComputation = _

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

  def init() = {
    
    // master computation
    masterComputation = config.createMasterComputation()
    masterComputation.setUnderlyingExecutionEngine(this)
    masterComputation.init()

    // master must know aggregators metadata
    val computation = config.createComputation()
    computation.initAggregations()

    // create one spark accumulator for each aggregation storage registered by
    // the computation via metadata
    def createAggregationAccum [K <: Writable, V <: Writable] (name: String,
        metadata: AggregationStorageMetadata[K,V]) = {
      sc.accumulator (new AggregationStorage[K,V](name))(new AggregationStorageParam[K,V](name))
    }
    aggAccums = Map.empty
    for ((name,metadata) <- config.getAggregationsMetadata)
      aggAccums.update (name, createAggregationAccum(name, metadata))

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
    val _aggAccums = aggAccums
    val configBc = sc.broadcast(config)
    val serHadoopConf = new SerializableConfiguration(sc.hadoopConfiguration)

    // setup an RDD to simulate empty partitions and a broadcast variable to
    // communicate the global aggregated ODAGs on each step
    val superstepRDD = sc.makeRDD (Seq.empty[Any], numPartitions).cache
    var aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,ODAG]] = sc.broadcast (Map.empty)

    var previousAggregationsBc: Broadcast[_] = sc.broadcast (
      //aggAccums.map {case (name,accum) => (name,accum.value)}
      Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]
    )

    val startTime = System.currentTimeMillis

    do {

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
      //execEngines.cache

      // materialize engines before proceed:
      // 1. it will expand embeddings locally on each partition
      // 2. the embeddings that survive filtering are packed into ODAGs, still
      //    locally
      // 3. the expansions (embeddings) may also feed local AggregationStorages
      execEngines.count

      // we choose the flush method for ODAGs: load-balancing vs. overhead
      val aggregatedOdags = config.getString ("flush_method", SparkConfiguration.FLUSH_BY_PATTERN) match {
        case SparkConfiguration.FLUSH_BY_PATTERN =>
          val odags = execEngines.flatMap (_.flushByPattern)
          aggregatedOdagsByPattern (odags)
        case SparkConfiguration.FLUSH_BY_ENTRIES =>
          val odags = execEngines.flatMap (_.flushByEntries)
          aggregatedOdagsByEntries (odags)
        case SparkConfiguration.FLUSH_BY_PARTS =>
          val odags = execEngines.flatMap (_.flushByParts)
          aggregatedOdagsByParts (odags)
      }

      /** Now we can submit the job that will flush and aggregate ODAGs
       *  (odagsFuture) and the set of jobs that will aggregate
       *  AggregationStorages. Naturally, at this point, this may happen
       *  concurrently. */

      // create futures (two jobs submitted roughly simultaneously)
      val odagsFuture = Future { aggregatedOdags.collectAsMap  }
      val aggregationsFuture = getAggregations (execEngines, numPartitions)

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

      //previousAggregationsBc = sc.broadcast (
      //  aggAccums.map {case (name,accum) => (name,accum.value)}
      //)

      // the exec engines have no use anymore, make room for the next round
      execEngines.unpersist()
      
      // whether the user chose to customize master computation, executed every
      // superstep
      masterComputation.compute()

      val superstepFinish = System.currentTimeMillis
      logInfo (s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms")
      
      superstep += 1

    } while (!sc.isStopped && !aggregatedOdagsBc.value.isEmpty) // while there are ODAGs to be processed

    val finishTime = System.currentTimeMillis

    logInfo (s"Computation has finished. It took ${finishTime - startTime} ms")

    //aggAccums.foreach { case (name,accum) =>
    //  logInfo (s"Accumulator/Aggregator [$name]\n${accum.value}")
    //}
  }

  /**
   * Creates an RDD of execution engines for the superstep
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
      odag.setSerializeAsWriteOnly (true)
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
      odag.setSerializeAsWriteOnly(true)
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
      tup._2.setSerializeAsWriteOnly (true)
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
      (implicit kt: ClassTag[K], vt: ClassTag[V]) = Future[AggregationStorage[_ <: Writable, _ <: Writable]] {

      val keyValues = execEngines.flatMap (execEngine =>
          execEngine.flushAggregationsByName(name).asInstanceOf[Iterator[(SerializableWritable[K],SerializableWritable[V])]]
          )
      val func = metadata.getReductionFunction.func.call _
      val aggStorage = keyValues.reduceByKey {(sw1, sw2) =>
        sw1.t = func(sw1.value, sw2.value)
        sw1
      }.map { case (swk, swv) =>
        val aggStorage = new AggregationStorage[K,V] (
          name, Map(swk.value -> swv.value)
        )
        aggStorage.endedAggregation
        aggStorage
      }.reduce { (agg1,agg2) =>
        agg1.aggregate (agg2)
        agg1
      }
      
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
    }

    aggregations
  }

  //override def getAggregatedValue[T <: Writable](name: String) = aggAccums.get(name) match {
  //  case Some(accum) => accum.value.asInstanceOf[T]
  //  case None =>
  //    logWarning (s"Aggregation/accumulator $name not found")
  //    null.asInstanceOf[T]
  //}

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
    sc.stop() // stop spark context, this is important
  }
}

/**
 * Companion object: static methods and fields
 */
object SparkMasterExecutionEngine {
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
