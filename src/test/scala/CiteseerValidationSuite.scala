package io.arabesque

import io.arabesque.conf.{Configuration, SparkConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.hadoop.fs
import org.apache.hadoop.fs.Path

class CiteseerValidationSuite extends FunSuite with BeforeAndAfterAll {

  import SparkConfiguration._
 private val master = "local[*]"
 private val appName = "arabesque-spark"

 private var sampleGraphPath: String = _
 private var sc: SparkContext = _
 private var arab: ArabesqueContext = _
 private var arabGraph: ArabesqueGraph = _
  private var validator: EmbeddingsValidator = _
  private val fsScheme = "hdfs://localhost:8020"

 /** set up spark context */
 override def beforeAll: Unit = {
   // spark conf and context
   val conf = new SparkConf().
     setMaster(master).
     setAppName(appName)

   sc = new SparkContext(conf)
   arab = new ArabesqueContext(sc, "info")

   /*
   val loader = classOf[CiteseerValidationSuite].getClassLoader
   val url = loader.getResource("hdfs://localhost:8020/input/citeseer.unsafe.graph")
   sampleGraphPath = url.getPath
   */

   //"hdfs://localhost:8020/output/citeseer_motifs4_original/*"
   sampleGraphPath = fsScheme + "/input/citeseer.unsafe.graph"
   //sampleGraphPath = "file:///input/citeseer.unsafe.graph"
   //sampleGraphPath = "hdfs:///input/citeseer.unsafe.graph"
   arabGraph = arab.textFile (sampleGraphPath, false)

   validator = new EmbeddingsValidator(sc)
 }

 /** stop spark context */
 override def afterAll: Unit = {
   if (sc != null) {
     sc.stop()
     arab.stop()
   }
 }

  def deleteIfExists(hdfsPath: String) = {
    //val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)
    val hdfs = fs.FileSystem.get(new java.net.URI(fsScheme), sc.hadoopConfiguration)
    val path = new fs.Path(hdfsPath)

    println("Path to delete: " + hdfsPath)
    println("Scheme: " + path.getFileSystem(sc.hadoopConfiguration).getScheme)
    println("Scheme: " + path.getFileSystem(sc.hadoopConfiguration).getUri)

    if(hdfs.exists(path))
      hdfs.delete(path,true)
  }

 /** tests */
 test("configurations") {
   // TODO: make this test more simple
   import scala.collection.mutable.Map
   val confs: Map[String,Any] = Map(
     "spark_master" -> "local[*]",
     "input_graph_path" -> sampleGraphPath,
     "input_graph_local" -> false,
     "computation" -> "io.arabesque.computation.BasicComputation"
     )

   val sparkConfig = new SparkConfiguration (confs)

   assert (!sparkConfig.isInitialized)

   sparkConfig.initialize
   assert (sparkConfig.getComputationClass ==
     Class.forName("io.arabesque.computation.BasicComputation"))
   assert (!Configuration.isUnset)

   val sparkConfBc = sc.broadcast (sparkConfig)
   val testingRDD = sc.parallelize (Seq.empty, 1)

   val conds = testingRDD.mapPartitions { _ =>
     var bools = List[Boolean]()
     val sparkConfig = sparkConfBc.value

     bools = sparkConfig.isInitialized :: bools

     sparkConfig.initialize
     bools = (sparkConfig.getMainGraph != null) :: bools

     bools = (!Configuration.isUnset) :: bools

     bools.iterator
   }

   assert (conds.reduce (_ && _))
 }

  val motifsSize = 4
  val motifsNumEmbeddings = 24546

  test (s"[motifs($motifsSize)]") {
    val groundTruthPath = fsScheme +  "/output/citeseer_motifs4_original"
    val outputPath = fsScheme + "/output/citeseer_motifs4"

    deleteIfExists(outputPath)

   val motifsRes = arabGraph.motifs (motifsSize)
     .set ("comm_strategy", COMM_ODAG_SP)
     .set ("output_active", true)
     .set ("flush_method", FLUSH_BY_PATTERN)
     .set ("output_path", outputPath)
     .set ("input_graph_local", false)
     .set (Configuration.CONF_SYSTEM_TYPE, Configuration.CONF_ARABESQUE_SYSTEM_TYPE)

   val embeddings = motifsRes.embeddings
   assert (embeddings.count == motifsNumEmbeddings)
   assert (embeddings.distinct.count == motifsNumEmbeddings)
    assert(validator.validate(groundTruthPath + "/*", outputPath + "/*", "VertexInduced"))
 }
} 
