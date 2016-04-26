package io.arabesque

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import io.arabesque.computation.SparkODAGMasterEngine$
import io.arabesque.conf.{Configuration, SparkConfiguration}

import io.arabesque._

// TODO: break these tests into several *suites*
class SparkArabesqueSuite extends FunSuite with BeforeAndAfterAll {

  import Configuration._
  import SparkConfiguration._

  private val master = "local[2]"
  private val appName = "arabesque-spark"
  
  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var arab: ArabesqueContext = _
  private var arabGraph: ArabesqueGraph = _

  /** set up spark context */
  override def beforeAll: Unit = {
    // configure log levels
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("io").setLevel(Level.ERROR)

    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)

    sc = new SparkContext(conf)
    arab = new ArabesqueContext(sc)

    val loader = classOf[SparkArabesqueSuite].getClassLoader
    val url = loader.getResource("sample.graph")
    sampleGraphPath = url.getPath
    arabGraph = arab.textFile (sampleGraphPath)

  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
      arab.stop()
    }
  }

  /** tests */
  test("configurations") {
    // TODO: make this test more simple
    import scala.collection.mutable.Map
    val confs: Map[String,Any] = Map(
      "spark_master" -> "local[2]",
      "input_graph_path" -> sampleGraphPath,
      "input_graph_local" -> true,
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

  val motifsNumEmbeddings = 24546
  test ("[motifs,odag] arabesque API") {
    val motifsRes = arabGraph.motifs (3).
      set ("comm_strategy", COMM_ODAG).
      set ("log_level", "debug")
    val odags = motifsRes.odags
    assert (odags.count != 0)
    val embeddings = motifsRes.embeddings
    assert (embeddings.count == motifsNumEmbeddings)
    assert (embeddings.distinct.count == motifsNumEmbeddings)
  }
  test ("[motifs,embedding] arabesque API") {
    val motifsRes = arabGraph.motifs (3).
      set ("log_level", "debug").
      set ("comm_strategy", COMM_EMBEDDING)
    val odags = motifsRes.odags
    assert (odags.count == 0)
    val embeddings = motifsRes.embeddings
    assert (embeddings.count == motifsNumEmbeddings)
    assert (embeddings.distinct.count == motifsNumEmbeddings)
  }

  val fsmNumEmbeddings = 31414
  test ("[fsm,odag] arabesque API") {
    val fsmRes = arabGraph.fsm (100, 3).
      set ("comm_strategy", COMM_ODAG)
    val odags = fsmRes.odags
    val embeddings = fsmRes.embeddings
    assert (embeddings.count == fsmNumEmbeddings)
    assert (embeddings.distinct.count == fsmNumEmbeddings)
  }
  test ("[fsm,embedding] arabesque API") {
    val fsmRes = arabGraph.fsm (100, 3).
      set ("comm_strategy", COMM_EMBEDDING)
    val odags = fsmRes.odags
    assert (odags.count == 0)
    val embeddings = fsmRes.embeddings
    assert (embeddings.count == fsmNumEmbeddings)
    assert (embeddings.distinct.count == fsmNumEmbeddings)
  }

  val trianglesNumEmbeddings = 0
  test ("[triangles,odag] arabesque API") {
    val trianglesRes = arabGraph.triangles().
      set ("comm_strategy", COMM_ODAG)
    val odags = trianglesRes.odags
    val embeddings = trianglesRes.embeddings
    assert (embeddings.count == trianglesNumEmbeddings)
    assert (embeddings.distinct.count == trianglesNumEmbeddings)
  }
  test ("[triangles,embedding] arabesque API") {
    val trianglesRes = arabGraph.triangles().
      set ("comm_strategy", COMM_EMBEDDING)
    val odags = trianglesRes.odags
    assert (odags.count == 0)
    val embeddings = trianglesRes.embeddings
    assert (embeddings.count == trianglesNumEmbeddings)
    assert (embeddings.distinct.count == trianglesNumEmbeddings)
  }

  val cliquesNumEmbeddings = 1166
  test ("[cliques,odag] arabesque API") {
    val cliquesRes = arabGraph.cliques (3).
      set ("comm_strategy", COMM_ODAG)
    val odags = cliquesRes.odags
    val embeddings = cliquesRes.embeddings
    assert (embeddings.count == cliquesNumEmbeddings)
    assert (embeddings.distinct.count == cliquesNumEmbeddings)
  }
  test ("[cliques,embedding] arabesque API") {
    val cliquesRes = arabGraph.cliques (3).
      set ("comm_strategy", COMM_EMBEDDING)
    val odags = cliquesRes.odags
    assert (odags.count == 0)
    val embeddings = cliquesRes.embeddings
    assert (embeddings.count == cliquesNumEmbeddings)
    assert (embeddings.distinct.count == cliquesNumEmbeddings)
  }
} 
