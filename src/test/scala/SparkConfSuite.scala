package io.arabesque

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import io.arabesque.computation.SparkMasterExecutionEngine
import io.arabesque.conf.{Configuration, SparkConfiguration}

class SparkConfSuite extends FunSuite with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "arabesque-spark"

  private var sc: SparkContext = _

  /** set up spark context */
  before {
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
  }

  /** stop spark context */
  after {
    if (sc != null) {
      sc.stop()
    }
  }

  /** tests */
  test("configurations") {
    
    val loader = classOf[SparkConfSuite].getClassLoader
    val url = loader.getResource("sample.graph")

    import scala.collection.mutable.Map
    val confs: Map[String,Any] = Map(
      "spark_master" -> "local[2]",
      "input_graph_path" -> url.getPath,
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

      println (bools)

      bools.iterator
    }

    assert (conds.reduce (_ && _))
    
  }

} 
