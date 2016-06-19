package io.arabesque.computation

import java.io.{ByteArrayInputStream, DataInputStream}

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding._
import io.arabesque.odag._
import io.arabesque.pattern.Pattern
import io.arabesque.utils.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.SizeEstimator

import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success}

/**
 * Underlying engine that runs the Arabesque master.
 * It interacts directly with the RDD interface in Spark by handling the
 * SparkContext.
 */ 
class ODAGMasterEngineSP [E <: Embedding] (_config: SparkConfiguration[E])
    (implicit val oTag: ClassTag[SinglePatternODAG])
    extends ODAGMasterEngine [E,SinglePatternODAG,SinglePatternODAGStash,ODAGEngineSP[E]] {
  
  def config: SparkConfiguration[E] = _config

  def this(_sc: SparkContext, config: SparkConfiguration[E]) {
    this (config)
    sc = _sc
    init()
  }

  def this(confs: Map[String,Any]) {
    this (new SparkConfiguration [E] (confs))

    sc = new SparkContext(config.sparkConf)
    val logLevel = config.getString ("log_level", "INFO").toUpperCase
    sc.setLogLevel (logLevel)

    init()
  }

  /**
   * Master's computation takes place here, superstep by superstep
   */
  override def compute() = {
    val numPartitions = config.getInteger ("num_partitions", 10)

    // accumulatores and spark configuration w.r.t. Spark
    // TODO: ship serHaddopConf with SparkConfiguration
    val configBc = sc.broadcast(config)
    val serHadoopConf = new SerializableConfiguration(sc.hadoopConfiguration)

    // setup an RDD to simulate empty partitions and a broadcast variable to
    // communicate the global aggregated ODAGs on each step
    val superstepRDD = sc.makeRDD (Seq.empty[Any], numPartitions).cache
    var aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,SinglePatternODAG]] =
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
      execEngines.persist (MEMORY_ONLY_SER)

      // Materialize execEngines
      execEngines.foreachPartition (_ => {})

      /** [1] We extract and aggregate the *aggregations* globally.
       *  That gives us the opportunity to do aggregationFilter in the generated
       *  ODAGs before collecting/broadcasting */

      // create futures (two jobs submitted roughly simultaneously)
      val aggregationsFuture = getAggregations (execEngines, numPartitions)
      // aggregations
      Await.ready (aggregationsFuture, atMost = Duration.Inf)
      aggregationsFuture.value.get match {
        case Success(previousAggregations) =>

          aggregations = mergeOrReplaceAggregations (aggregations, previousAggregations)

          logInfo (s"""Aggregations and sizes
            ${previousAggregations.
            map(tup => (tup._1,tup._2)).mkString("\n")}
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
      val aggregatedOdags = config.
        getString ("flush_method", SparkConfiguration.FLUSH_BY_PATTERN) match {
        case SparkConfiguration.FLUSH_BY_PATTERN =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flush).
            asInstanceOf[RDD[(Pattern,SinglePatternODAG)]]
          aggregatedOdagsByPattern (odags)

        case SparkConfiguration.FLUSH_BY_ENTRIES =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flush).
            asInstanceOf[RDD[((Pattern,Int,Int), SinglePatternODAG)]]
          aggregatedOdagsByEntries (odags)

        case SparkConfiguration.FLUSH_BY_PARTS =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flush).
            asInstanceOf[RDD[((Pattern,Int),Array[Byte])]]
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

          /* maybe debug odag stats */
          if (log.isDebugEnabled) {
            for ((pattern,odag) <- aggregatedOdagsLocal.iterator) {
              val storage = odag.getStorage
              storage.finalizeConstruction
              logDebug (
                s"Superstep{${superstep}}" +
                s";Patterns{1}" +
                s";StorageEstimate{${SizeEstimator.estimate (odag.getStorage)}}" +
                s";PatternEstimate{${SizeEstimator.estimate (pattern)}}" +
                s";${storage.toStringResume}" +
                s";${storage.getStats}" +
                s";${storage.getStats.getSizeEstimations}"
              )
            }
          }

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
  private def getExecutionEngines(
      superstepRDD: RDD[Any],
      superstep: Int,
      configBc: Broadcast[SparkConfiguration[E]],
      aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,SinglePatternODAG]],
      serHadoopConf: SerializableConfiguration,
      aggAccums: Map[String,Accumulator[_]],
      previousAggregationsBc: Broadcast[_]) = {

    // read embeddings from global agg. ODAGs, expand, filter and process
    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, _) =>

      configBc.value.initialize()

      val execEngine = new ODAGEngineSP [E] (
        partitionId = idx,
        superstep = superstep,
        hadoopConf = serHadoopConf,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc
      )
      execEngine.init()
      val stash = new SinglePatternODAGStash (aggregatedOdagsBc.value)
      execEngine.compute (Iterator (stash))
      execEngine.finalize()
      Iterator(execEngine)
    }

    execEngines
  }

  private def aggregatedOdagsByPattern(odags: RDD[(Pattern,SinglePatternODAG)]) = {

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


  private def aggregatedOdagsByEntries(odags: RDD[((Pattern,Int,Int),SinglePatternODAG)]) = {

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
        val _odag = new SinglePatternODAG(false)
        _odag.readFields (dataInput)
        _odag
      },
      (odag: SinglePatternODAG, byteArray: Array[Byte]) => {
        val dataInput = new DataInputStream(new ByteArrayInputStream(byteArray))
        val _odag = new SinglePatternODAG(false)
        _odag.readFields (dataInput)
        odag.aggregate (_odag)
        odag
      },
      (odag1: SinglePatternODAG, odag2: SinglePatternODAG) => {
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
}
