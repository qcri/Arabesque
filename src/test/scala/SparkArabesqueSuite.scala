package io.arabesque

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import io.arabesque.computation.SparkMasterExecutionEngine
import io.arabesque.conf.{Configuration, SparkConfiguration}

import io.arabesque._

// TODO: break these tests into several *suites*
class SparkArabesqueSuite extends FunSuite with BeforeAndAfterAll {

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
    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)
    //Logger.getLogger("io").setLevel(Level.ERROR)

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

  test ("[motifs] arabesque API") {
    val motifsRes = arabGraph.motifs (4).
      set ("output_path", s"target/${sc.applicationId}/Motifs_Output").
      set ("log_level", "debug")
    val odags = motifsRes.odags
    val embeddings = motifsRes.embeddings
    //assert (odags.count == 2)
    //assert (embeddings.count == 24546)
  }

  //test ("[fsm] arabesque API") {
  //  val fsmRes = arabGraph.fsm (100, 3).
  //    set ("output_path", s"target/${sc.applicationId}/FSM_Output")
  //  val odags = fsmRes.odags
  //  val embeddings = fsmRes.embeddings
  //  assert (odags.count == 3)
  //  assert (embeddings.count == 31414)
  //}

  //test ("[triangles] arabesque API") {
  //  val trianglesRes = arabGraph.triangles().
  //    set ("output_path", s"target/${sc.applicationId}/Triangles_Output")
  //  val odags = trianglesRes.odags
  //  val embeddings = trianglesRes.embeddings
  //  assert (odags.count == 2)
  //  assert (embeddings.count == 0)
  //}

  //test ("[cliques] arabesque API") {
  //  val cliquesRes = arabGraph.cliques (3).
  //    set ("output_path", s"target/${sc.applicationId}/Cliques_Output")
  //  val odags = cliquesRes.odags
  //  val embeddings = cliquesRes.embeddings
  //  assert (odags.count == 2)
  //  assert (embeddings.count == 1166)
  //}

} 
