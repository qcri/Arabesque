/* ArabesqueTest.scala */

package io.arabesque.computation

import org.apache.spark.{Logging, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Accumulator, Accumulable}
import org.apache.spark.{AccumulatorParam, AccumulableParam}

import org.apache.spark.util.SizeEstimator

import org.apache.hadoop.io.{Writable, LongWritable, IntWritable}

import io.arabesque.graph.BasicMainGraph

import io.arabesque.odag.{ODAG, ODAGStash}
import io.arabesque.embedding.{Embedding, VertexInducedEmbedding}
import io.arabesque.pattern.Pattern

import io.arabesque.conf.{Configuration, SparkConfiguration}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import java.util.concurrent.{ExecutorService, Executors}
import java.io.{DataOutput, ByteArrayOutputStream, DataOutputStream, OutputStream,
                DataInput, ByteArrayInputStream, DataInputStream, InputStream}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */
class SparkMasterExecutionEngine(confs: Map[String,String]) extends
    CommonMasterExecutionEngine with Logging {

  private val conf = new SparkConf().setAppName("Arabesque Master Execution Engine")


  // TODO would be interesting to make Kryo the default serializer for Spark
  // however it would require great changes in the way classes are serialized by
  // default (Serializable vs. Externalizable vs. Writable for Hadoop)
  //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf.registerKryoClasses(Array(
  //  classOf[ODAG]
  //  ))

  private val sc = new SparkContext(conf)
  sc.setLogLevel ("INFO")

  private val sparkConf = new SparkConfiguration(confs)
  sparkConf.initialize()

  // counting of motifs
  private var accums: Map[String,Accumulable[Map[Pattern,Long], (Pattern,Long)]] = Map(
    "motifs" -> sc.accumulable(Map.empty[Pattern,Long], "motifs")(PatternLongAccumParam)
  )

  private var superstep = 0

  def init() = { }

  override def haltComputation() = {
    sc.stop()
  }

  override def getSuperstep(): Long = superstep

  /**
   * Master's computation takes place here, superstep by superstep
   */
  def compute() = {

    // accumulatores and spark configuration w.r.t. Spark
    val numEmbeddings = sc.accumulator(0L, "numEmbeddings")
    val _accums = accums
    val sparkConfBc = sc.broadcast(sparkConf)

    // setup an RDD to simulate empty partitions and a broadcast variable to
    // communicate the global aggregated ODAGs on each step
    val superstepRDD = sc.makeRDD (Seq.empty[Any], sparkConf.numPartitions)
    var globalAggBc: Broadcast[scala.collection.Map[Pattern,ODAG]] = sc.broadcast (Map.empty)

    do {
      val _superstep = superstep

      // read embeddings from global agg. ODAGs, expand, filter and process
      val odags = superstepRDD.mapPartitionsWithIndex { (idx, _) =>

        sparkConfBc.value.initialize()

        val execEngine = new SparkExecutionEngine(idx, _superstep, _accums, numEmbeddings)
        execEngine.init()
        execEngine.compute (Iterator (new ODAGStash(globalAggBc.value)))
        execEngine.finalize()

        // TODO three options currently available to communicate ODAGs
        //execEngine.flushInParts
        //execEngine.flush
        execEngine.flushOutputs
      }

      // (flushInParts) ODAGs' reduction by pattern as a key
      //val globalAgg = odags.reduceByKey { (odag1, odag2) =>
      //  odag1.aggregate (odag2)
      //  odag1
      //// resulting ODAGs must be deserialized for read(only)
      //}.
      //map { case ((pattern,_,_), odag) =>
      //  (pattern, odag)
      //}.reduceByKey { (odag1, odag2) =>
      //  odag1.aggregate (odag2)
      //  odag1
      //}.
      //map { case (pattern,odag) =>
      //  odag.setSerializeAsWriteOnly(true)
      //  (pattern,odag)
      //}
      

      // (flush)
      //val globalAgg = odags.reduceByKey { (odag1, odag2) =>
      //  odag1.aggregate (odag2)
      //  odag1
      //}.
      //map { case (pattern, odag) =>
      //  odag.setSerializeAsWriteOnly (true)
      //  (pattern, odag)
      //}

      // (flushOutputs)
      val globalAgg = odags.combineByKey (
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
      
      // collect and broadcast new generation of ODAGs
      val globalAggLocal = globalAgg.collectAsMap
      globalAggBc.unpersist()
      globalAggBc = sc.broadcast (globalAggLocal)
      logInfo (s".superstep $superstep ended.")
      logInfo (globalAggLocal.toString)
      superstep += 1

    } while (!globalAggBc.value.isEmpty) // while there are ODAGs to be processed

    logInfo (".computation has ended. Num embeddings = " + numEmbeddings.value)

  }

  override def finalize() = {

    // log the accumulators' results
    accums.foreach {case (name,accum) =>
      accum.value.toSeq.sortBy(_._2).foreach {case (k,v) =>
        logInfo (s"$name: $k -> $v")
      }
    }
    super.finalize()
    sc.stop() // stop spark context, this is important
  }
}

/**
 * Companion object with the main function
 * TODO an integration with ArabesqueRunner would deprecate this
 */
object SparkMasterExecutionEngine {
  def main(args: Array[String]) {
    val confs: Map[String,String] = Map.empty ++ args.map {str =>
      val arr = str split "="
      (arr(0), arr(1))
    }.toMap

    val masterEngine = new SparkMasterExecutionEngine(confs)
    masterEngine.compute
    masterEngine.finalize
  }
}

// ad-hoc accumulators (aka things that are aggregated in the master) for the
// motif-problem
abstract class ArabesqueAccumulatorParam[K,V] extends AccumulableParam[Map[K,V], (K,V)] {
  def zero(initialValue: Map[K,V]): Map[K,V] = initialValue
}

object PatternLongAccumParam extends ArabesqueAccumulatorParam[Pattern,Long] {

  def addInPlace(v1: Map[Pattern,Long], v2: Map[Pattern,Long]): Map[Pattern,Long] = {
    var (merged,toMerge) = if (v1.size > v2.size) (v1,v2) else (v2,v1)
    for ((k,v) <- toMerge.iterator) merged.get(k) match {
      case Some(_v) =>
        merged += (k -> (_v + v))
      case None =>
        merged += (k -> v)
    }
    merged
  }

  def addAccumulator(acc: Map[Pattern,Long], elem: (Pattern,Long)) = {
    var toMerge = acc
    val (k,v) = elem
    acc.get(k) match {
      case Some(_v) =>
        toMerge += (k -> (_v + v))
      case None =>
        toMerge += (k -> v)
    }
    toMerge
  }

}
