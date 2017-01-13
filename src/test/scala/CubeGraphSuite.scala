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
    // spark conf and context
    val conf = new SparkConf().
      setMaster(master).
      setAppName(appName)

    sc = new SparkContext(conf)
    arab = new ArabesqueContext(sc, "warn")

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

  test ("[motifs] arabesque API") {
    // Test output for motifs for embedding with size 0 to 3

    // Expected output
    val numEmbedding = List(0, 8, 12, 24)

    for(k <- 0 to (numEmbedding.size - 1)) {
      val motifsRes = arabGraph.motifs(k).
        set ("num_partitions", 10)
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
      val cliqueRes = arabGraph.cliques(k)

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
      val motifsRes = arabGraph.fsm(support, k)

      val embeddings = motifsRes.embeddings

      assert(embeddings.count == numEmbedding(k))
    }

  }


  test ("[triangles] arabesque API") {
    // Test output for triangles

    // Expected output
    val numTriangles = 0

    val trianglesRes = arabGraph.triangles()

    val embeddings = trianglesRes.embeddings

    assert(embeddings.count == numTriangles)
  }

}
