package io.arabesque.compression

import java.io.{ByteArrayInputStream, DataInputStream}

import io.arabesque.embedding.Embedding
import io.arabesque.aggregation.AggregationStorage
import io.arabesque.conf.SparkConfiguration
import io.arabesque.pattern.Pattern
import io.arabesque.report._
import org.apache.hadoop.io.Writable
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
import scala.util.{Failure, Success}

/**
  * Created by ehussein on 7/17/17.
  */
class SimpleStorageMasterEngineSP [E <: Embedding] (_config: SparkConfiguration[E])
                                                            (implicit val oTag: ClassTag[SinglePatternSimpleStorage])
  extends SimpleStorageMasterEngine [E,SinglePatternSimpleStorage,SinglePatternSimpleStorageStash, SimpleStorageEngineSP[E]]{

  def config: SparkConfiguration[E] = _config

  val enumsFilePath = "/home/ehussein/Downloads/ArabesqueTesting/compresssion/storage_enums/enums"

  def this(_sc: SparkContext, config: SparkConfiguration[E]) {
    this (config)
    sc = _sc
    init()
  }

  def this(confs: Map[String,Any]) {
    this (new SparkConfiguration [E] (confs))
    sc = new SparkContext(config.sparkConf)
    init()
  }

  /**
    * Master's computation takes place here, superstep by superstep
    */
  override def compute() = {
    // accumulatores and spark configuration w.r.t. Spark
    val configBc = sc.broadcast(config)
    logInfo (s"SparkConfiguration estimated size = ${SizeEstimator.estimate(config)} bytes")
    logInfo (s"HadoopConfiguration estimated size = ${SizeEstimator.estimate(config.hadoopConf)} bytes")

    // setup an RDD to simulate empty partitions and a broadcast variable to
    // communicate the global aggregated ODAGs on each step
    val superstepRDD = sc.makeRDD (Seq.empty[Any], numPartitions).cache
    var aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,SinglePatternSimpleStorage]] =
      sc.broadcast (Map.empty)

    var previousAggregationsBc: Broadcast[_] = sc.broadcast (
      Map.empty[String,AggregationStorage[_ <: Writable, _ <: Writable]]
    )

    val startTime = System.currentTimeMillis

    do {
      val masterReport: MasterReport = new MasterReport
      masterReport.superstep = superstep
      masterReport.startTime = System.currentTimeMillis

      /*
      if(superstep == 3) {
        // Printing ODAGs at the beginning of each super step
        println(s"Printing ODAGs at the beginning of super_step($superstep)")
        var total:Long = 0L
        aggregatedOdagsBc.value.foreach(odagStash => {
          println("Pattern: " + odagStash._1.toOutputString)
          println("ODAG: " + odagStash._2.toString)
          total += odagStash._2.getNumberOfEmbeddings
        })
        println("Total number of embeddings = " + total)
        //
      }
      */

      val _aggAccums = aggAccums
      val superstepStart = System.currentTimeMillis

      val execEngines = getExecutionEngines (
        superstepRDD = superstepRDD,
        superstep = superstep,
        configBc = configBc,
        aggregatedOdagsBc = aggregatedOdagsBc,
        aggAccums = _aggAccums,
        previousAggregationsBc = previousAggregationsBc)

      // keep engines (filled with expansions and aggregations) for the rest of
      // the superstep
      execEngines.persist (MEMORY_ONLY)

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
      val aggregatedOdags = config.getOdagFlushMethod match {
        case SparkConfiguration.FLUSH_BY_PATTERN =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flush).
            asInstanceOf[RDD[(Pattern,SinglePatternSimpleStorage)]]
          aggregatedStorageByPattern (odags)

        case SparkConfiguration.FLUSH_BY_ENTRIES =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flush).
            asInstanceOf[RDD[((Pattern,Int,Int), SinglePatternSimpleStorage)]]
          aggregatedStorageByEntries (odags)

        case SparkConfiguration.FLUSH_BY_PARTS =>
          val odags = execEngines.
            map (_.withNewAggregations (previousAggregationsBc)). // update previousAggregations
            flatMap (_.flush).
            asInstanceOf[RDD[((Pattern,Int),Array[Byte])]]
          aggregatedStorageByParts (odags)
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
            var totalStorageEstimate:Long = 0
            var totalPatternEstimate:Long = 0

            for ((pattern,odag) <- aggregatedOdagsLocal.iterator) {
              val storage = odag.getStorage
              storage.finalizeConstruction

              val storageEstimate = SizeEstimator.estimate (odag.getStorage)
              val patternEstimate = SizeEstimator.estimate (pattern)

              logDebug (
                s"\n\nSuperstep{$superstep}" +
                  s";\nPatterns{1}" +
                  s";\nStorageEstimate{${storageEstimate}}" +
                  s";\nPatternEstimate{${patternEstimate}}" +
                  s";\n\n${storage.toStringResume}" +
                  s";\n\n${storage.getStats}" +
                  s";\n\n${storage.getStats.getSizeEstimations}"
              )

              totalStorageEstimate += storageEstimate
              totalPatternEstimate += patternEstimate
            }

            // print totals
            logDebug(
              s"\n\nTotalStorageEstimation=$totalStorageEstimate" +
                s"\nTotalPatternEstimation=$totalPatternEstimate\n"
            )
          }

        case Failure(e) =>
          logError (s"Error in collecting odags ${e.getMessage}")
          throw e
      }

      // the exec engines have no use anymore, make room for the next round
      execEngines.unpersist()

      // whether the user chose to customize master computation, executed every superstep
      masterComputation.compute()

      val superstepFinish = System.currentTimeMillis
      logInfo (s"Superstep $superstep finished in ${superstepFinish - superstepStart} ms")
      logInfo(s"${aggregatedOdagsBc.toString()}")

      // print the stash
      /*
      if(superstep <= 4) {
        val stash = new SinglePatternSimpleStorageStash(aggregatedOdagsBc.value)
        stash.printAllEnumerations(s"${enumsFilePath}_SuperStep$superstep")
      }
      */
      //

      // print stats
      aggAccums = aggAccums.map { case (name,accum) =>
        logInfo (s"Accumulator[$name]: ${accum.value}")
        (name -> sc.accumulator [Long] (0L, name))
      }

      // calc storage size/summary for master report for this superstep
      var i = 0
      aggregatedOdagsBc.value.foreach(entry => {
        val pattern = entry._1
        val odag = entry._2
        val storage = odag.getStorage
        storage.finalizeConstruction
        val storageEstimate = SizeEstimator.estimate (storage)
        val patternEstimate = SizeEstimator.estimate (pattern)
        masterReport.storageSize += storageEstimate
        masterReport.patternSize += patternEstimate
        masterReport.storageSummary += storage.toStringResume
        i += 1
      })

      masterReport.endTime = System.currentTimeMillis()
      if(generateReports)
        masterReport.saveReport(reportsFilePath)

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
                                   aggregatedOdagsBc: Broadcast[scala.collection.Map[Pattern,SinglePatternSimpleStorage]],
                                   aggAccums: Map[String,Accumulator[_]],
                                   previousAggregationsBc: Broadcast[_]) = {

    // read embeddings from global agg. ODAGs, expand, filter and process
    val execEngines = superstepRDD.mapPartitionsWithIndex { (idx, _) =>

      configBc.value.initialize()

      val execEngine = new SimpleStorageEngineSP [E] (
        partitionId = idx,
        superstep = superstep,
        accums = aggAccums,
        previousAggregationsBc = previousAggregationsBc
      )
      execEngine.init()
      val stash = new SinglePatternSimpleStorageStash(aggregatedOdagsBc.value)
      execEngine.compute (Iterator (stash))
      execEngine.finalize()
      Iterator(execEngine)
    }

    execEngines
  }

  private def aggregatedStorageByPattern(odags: RDD[(Pattern,SinglePatternSimpleStorage)]) = {

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


  private def aggregatedStorageByEntries(odags: RDD[((Pattern,Int,Int),SinglePatternSimpleStorage)]) = {

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


  private def aggregatedStorageByParts(odags: RDD[((Pattern,Int), Array[Byte])]) = {

    // (flushByParts)
    val aggregatedOdags = odags.combineByKey (
      (byteArray: Array[Byte]) => {
        val dataInput = new DataInputStream(new ByteArrayInputStream(byteArray))
        val _odag = new SinglePatternSimpleStorage(false)
        _odag.readFields (dataInput)
        _odag
      },
      (odag: SinglePatternSimpleStorage, byteArray: Array[Byte]) => {
        val dataInput = new DataInputStream(new ByteArrayInputStream(byteArray))
        val _odag = new SinglePatternSimpleStorage(false)
        _odag.readFields (dataInput)
        odag.aggregate (_odag)
        odag
      },
      (odag1: SinglePatternSimpleStorage, odag2: SinglePatternSimpleStorage) => {
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
