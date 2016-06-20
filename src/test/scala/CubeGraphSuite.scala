package io.arabesque

import io.arabesque.conf.{Configuration, SparkConfiguration}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class CubeGraphSuite extends FunSuite with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = "arabesque-spark"
  
  private var sampleGraphPath: String = _
  private var sc: SparkContext = _
  private var arab: ArabesqueContext = _
  private var arabGraph: ArabesqueGraph = _

  /** set up spark context */
  override def beforeAll: Unit = {
    // configure log levels
    import org.apache.log4j.{Level, Logger}
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

    sampleGraphPath = "data/cube.graph"
    arabGraph = arab.textFile (sampleGraphPath)

  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
      arab.stop()
    }
  }

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
    // Test output for motifs for embedding with size 0 to 3

    // Expected output
    val numEmbedding = List(0, 8, 12, 24)

    for(k <- 0 to (numEmbedding.size - 1)) {
      val motifsRes = arabGraph.motifs(k).
        set ("log_level", "debug")
      val odags = motifsRes.odags
      val embeddings = motifsRes.embeddings

      assert(embeddings.count() == numEmbedding(k))
    }

  }

  test ("[clique] arabesque API") {
    // Test output for clique for embeddings with size 1 to 3
    // Expected output
    val numEmbedding = List(0, 8, 12, 0)

    for(k <- 0 to (numEmbedding.size - 1)) {
      val cliqueRes = arabGraph.cliques(k).
        set ("log_level", "debug")

      val embeddings = cliqueRes.embeddings

      assert(embeddings.count == numEmbedding(k))
    }

  }


  test ("[fsm] arabesque API") {
    // Critical test
    // Test output for fsm with support 2 for embeddings with size 2 to 3
    val support = 2

    // Expected output
    val numEmbedding = List(0, 0, 9, 24)

    for(k <- 0 to (numEmbedding.size -1)) {
      val motifsRes = arabGraph.fsm(support, k).
        set ("log_level", "debug")

      val embeddings = motifsRes.embeddings

      assert(embeddings.count == numEmbedding(k))
    }

  }


  test ("[triangles] arabesque API") {
    // Test output for triangles

    // Expected output
    val numTriangles = 0

    val trianglesRes = arabGraph.triangles().
      set ("log_level", "debug")

    val embeddings = trianglesRes.embeddings

    assert(embeddings.count == numTriangles)
  }

}
