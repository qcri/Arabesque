package io.arabesque.compression

import java.nio.file.Paths

import io.arabesque.aggregation.{AggregationStorage, AggregationStorageMetadata}
import io.arabesque.computation.{MasterComputation, SparkMasterEngine}
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding.Embedding
import io.arabesque.odag.BasicODAG
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{Failure, Success}
import scala.collection.JavaConversions._

/**
  * Created by ehussein on 7/16/17.
  */
trait SimpleStorageMasterEngine [
E <: Embedding,
O <: SimpleStorage,
S <: SimpleStorageStash[O,S],
C <: SimpleStorageEngine[E,O,S,C]
] extends SparkMasterEngine [E] {

  implicit def oTag: ClassTag[O] // workaround to pass classtags to traits

  // sub-classes must implement
  def config: SparkConfiguration[E]

  // #reporting
  /*
  var reportsFilePath: String = _
  var generateReports: Boolean = false
  */

  import SimpleStorageMasterEngine._

  // testing
  config.initialize()

  // Spark accumulators for stats counting (non-critical)
  // Ad-hoc arabesque approach for user-defined aggregations
  var aggAccums: Map[String,Accumulator[_]] = _
  var aggregations
  : Map[String,AggregationStorage[_ <: Writable, _ <: Writable]] = _

  var superstep = 0

  var masterComputation: MasterComputation = _

  var odags: List[RDD[O]] = List()

  def sparkContext: SparkContext = sc
  def arabConfig: SparkConfiguration[_ <: Embedding] = config

  override def init() = {
    // guarantees that outputPath does not exist
    if (config.isOutputActive) {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val outputPath = new Path(config.getOutputPath)
      if (fs.exists (outputPath))
        throw new RuntimeException (
          s"Output path ${config.getOutputPath} exists. Choose another one."
        )
    }

    // #reporting
    /*
    // set reports path
    if(config.getBoolean("reports_active", false)) {
      reportsFilePath = config.getString("reports_path", Paths.get("").toAbsolutePath.normalize.toString)
      reportsFilePath += "/Master/"
      generateReports = true
    }
    */

    // master computation
    masterComputation = config.createMasterComputation()
    masterComputation.setUnderlyingExecutionEngine(this)
    masterComputation.init()

    // master must know aggregators metadata
    val computation = config.createComputation()
    computation.initAggregations()

    // stats aggregation via accumulators
    aggAccums = Map.empty
    aggAccums.update (AGG_EMBEDDINGS_PROCESSED,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_PROCESSED))
    aggAccums.update (AGG_EMBEDDINGS_GENERATED,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_GENERATED))
    aggAccums.update (AGG_EMBEDDINGS_OUTPUT,
      sc.accumulator [Long] (0L, AGG_EMBEDDINGS_OUTPUT))
    aggAccums.update (AGG_SPURIOUS_EMBEDDINGS,
      sc.accumulator [Long] (0L, AGG_SPURIOUS_EMBEDDINGS))

    super.init()
  }

  override def haltComputation() = {
    logInfo ("Halting master computation")
    sc.stop()
  }

  override def getSuperstep(): Long = superstep

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
                       execEngines: RDD[C],
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

  override def getOdags: RDD[_ <: BasicODAG] = {
    //sc.union (odags.toSeq)
    // TODO to be implemented later, we can do some refactoring between ODAG and SimpleStorage classes
    // We can not return a value because O :< SimpleStorage is not a subclass of BasicODAG
    null
  }
  /*
  override def getOdags: RDD[O] = {
    //sc.union (odags.toSeq)
    // TODO to be implemented later, we can do some refactoring between ODAG and SimpleStorage classes
    // We can not return a value because O :< SimpleStorage is not a subclass of BasicODAG
    null
  }
  */
}

object SimpleStorageMasterEngine {
  val AGG_EMBEDDINGS_PROCESSED = "embeddings_processed"
  val AGG_EMBEDDINGS_GENERATED = "embeddings_generated"
  val AGG_EMBEDDINGS_OUTPUT = "embeddings_output"
  val AGG_SPURIOUS_EMBEDDINGS = "embeddings_spurious"
}