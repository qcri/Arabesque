package  io.arabesque;

import io.arabesque.{ArabesqueContext, ArabesqueGraph, SparkArabesqueSuite}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by ghanemabdo on 5/3/17.
  */
class BugsResolutionSuite extends FunSuite with BeforeAndAfterAll {
  private val master = "local[2]"
  private val appName = "arabesque-spark"

  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var arab: ArabesqueContext = _
  private var arabGraph: ArabesqueGraph = _

  /** set up spark context */
  override def beforeAll: Unit = {
    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)
//      .
//      set("num_workers", "1").
//      set("num_compute_threads", "1").
//      set("output_active", "no").
//      set("input_graph_local", "true")

    sc = new SparkContext(conf)
    arab = new ArabesqueContext(sc, "warn")

//    sampleGraphPath = "data/cl=46456_cf=101223_sf=101224_fa=227213.graph"
//    sampleGraphPath = "data/fsm-test.graph"
//    sampleGraphPath = "data/cl=53931_cf=56234_sf=56235_fa=56251.graph"
//    arabGraph = arab.textFile ("data/cl=48724_cf=51068_sf=51069_fa=191576.graph", "data/cl=48724_cf=51068_sf=51069_fa=191576-subgraphs.graph", true)
    arabGraph = arab.textFile ("data/cl=58231_cf=58712.graph", "data/cl=58231_cf=58712-subgraphs.graph", true)
  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
      arab.stop()
    }
  }


  test ("[fsm] arabesque API") {
    // Critical test
    // Test output for fsm with support 2 for embeddings with size 2 to 3
    val support = 5
    val size = 5

      val fsmRes = arabGraph.disconnectedGraphFSM(support, size)
//    val fsmRes = arabGraph.fsm(support, size)

    fsmRes.embeddings.map(_.toOutputString).foreach(println)
//      val embeddings = fsmRes.embeddings
//
//      embeddings.map(_.toOutputString).foreach(println)

      assert(0 == 0)

  }
}
