/* ArabesqueTest.scala */

package io.arabesque.computation

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, Accumulable}
import org.apache.spark.{AccumulatorParam, AccumulableParam}
import org.apache.spark.rdd.RDD

import org.apache.spark.util.SizeEstimator

import org.apache.hadoop.io.{Writable, LongWritable, IntWritable}

import io.arabesque.graph.BasicMainGraph

import io.arabesque.odag.{ODAG, ODAGStash}
import io.arabesque.embedding.{Embedding, VertexInducedEmbedding}
import io.arabesque.pattern.Pattern

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.conf.Configuration._

import scala.collection.mutable.Map
import scala.collection.JavaConversions._

import java.util.concurrent.{ExecutorService, Executors}
import java.io.{DataOutput, ByteArrayOutputStream, DataOutputStream, OutputStream,
                DataInput, ByteArrayInputStream, DataInputStream, InputStream}

import scala.reflect.ClassTag

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkMasterExecutionEngine(confs: Map[String,Any]) extends
    CommonMasterExecutionEngine with Logging {

  // TODO would be interesting to make Kryo the default serializer for Spark
  // however it would require great changes in the way classes are serialized by
  // default (Serializable vs. Externalizable vs. Writable for Hadoop)
  //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf.registerKryoClasses(Array(
  //  classOf[ODAG]
  //  ))
  
  private var sparkConf: SparkConfiguration[_ <: Embedding] = _

  private var sc: SparkContext = _

  private var aggAccums: Map[String,Accumulator[_]] = _

  private var superstep = 0

  private var masterComputation: MasterComputation = _

  def init() = {
    sparkConf = new SparkConfiguration(confs)
    sparkConf.initialize()

    sc = new SparkContext(sparkConf.nativeSparkConf)
    val logLevel = sparkConf.getString ("log_level", "INFO").toUpperCase
    sc.setLogLevel (logLevel)

    // master computation
    masterComputation = sparkConf.createMasterComputation()
    masterComputation.setUnderlyingExecutionEngine(this)
    masterComputation.init()

    // master must know aggregators metadata
    val computation = sparkConf.createComputation()
    computation.initAggregations()

    // create one spark accumulator for each aggregation storage registered by
    // the computation via metadata
    def createAggregationAccum[K <: Writable, V <: Writable](name: String, metadata: AggregationStorageMetadata[K,V]) = {
      sc.accumulator (new AggregationStorage[K,V](name)) (new AggregationStorageParam[K,V](name))
    }
    aggAccums = Map.empty
    for ((name,metadata) <- sparkConf.getAggregationsMetadata)
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
    val numPartitions = sparkConf.getInteger ("num_partitions", 10)

    // accumulatores and spark configuration w.r.t. Spark
    val _aggAccums = aggAccums
    val sparkConfBc = sc.broadcast(sparkConf)

    // setup an RDD to simulate empty partitions and a broadcast variable to
    // communicate the global aggregated ODAGs on each step
    val superstepRDD = sc.makeRDD (Seq.empty[Any], numPartitions).cache
    var aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,ODAG]] = sc.broadcast (Map.empty)

    var previousAggregationsBc: Broadcast[_] = sc.broadcast (
      aggAccums.map {case (name,accum) => (name,accum.value)}
    )

    val startTime = System.currentTimeMillis

    def getExecutionEngine (superstep: Int) = {

      // read embeddings from global agg. ODAGs, expand, filter and process
      val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, _) =>

        sparkConfBc.value.initialize()

        val execEngine = new SparkExecutionEngine(idx, superstep, _aggAccums, previousAggregationsBc)
        execEngine.init()
        execEngine.compute (Iterator (new ODAGStash(aggregatedOdagsBc.value)))
        execEngine.finalize()
        Iterator(execEngine)
      }

      execEngines
    }

    do {

      val superstepStart = System.currentTimeMillis

      val execEngines = getExecutionEngine (superstep)

      val aggregatedOdags = sparkConf.getString ("flush_method", SparkConfiguration.FLUSH_BY_PATTERN) match {
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
      
      // collect and broadcast new generation of ODAGs
      // this is the point of synchronization of the current superstep
      val aggregatedOdagsLocal = aggregatedOdags.collectAsMap
     
      aggregatedOdagsBc.unpersist()
      previousAggregationsBc.unpersist()
      aggregatedOdagsBc = sc.broadcast (aggregatedOdagsLocal)
      previousAggregationsBc = sc.broadcast (
        aggAccums.map {case (name,accum) => (name,accum.value)}
      )
      
      // whether the user chose to customize master computation, executed every
      // superstep
      masterComputation.compute()

      val superstepFinish = System.currentTimeMillis
      logInfo (s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms")
      logInfo (s"Number of aggregated ODAGs = ${aggregatedOdagsLocal.size}")
      
      superstep += 1

    } while (!aggregatedOdagsBc.value.isEmpty) // while there are ODAGs to be processed

    val finishTime = System.currentTimeMillis

    logInfo ("Computation has finished")
    logInfo (s"Computation took ${finishTime - startTime} ms")

    aggAccums.foreach { case (name,accum) =>
      logInfo (s"Accumulator/Aggregator [$name]\n${accum.value}")
    }

  }

  def aggregatedOdagsByEntries(odags: RDD[((Pattern,Int,Int), ODAG)]) = {

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
    

  def aggregatedOdagsByPattern(odags: RDD[(Pattern, ODAG)]) = {

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

  def aggregatedOdagsByParts(odags: RDD[((Pattern,Int), Array[Byte])]) = {

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
  
  override def getAggregatedValue[T <: Writable](name: String) = aggAccums.get(name) match {
    case Some(accum) => accum.value.asInstanceOf[T]
    case None =>
      logWarning (s"Aggregation/accumulator $name not found")
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
