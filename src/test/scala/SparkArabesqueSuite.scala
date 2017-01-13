package io.arabesque

import io.arabesque.computation._
import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding._

import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.{BeforeAndAfterAll, FunSuite}

class SparkArabesqueSuite extends FunSuite with BeforeAndAfterAll {

  import SparkConfiguration._
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
     set ("comm_strategy", COMM_ODAG_SP)
   val odags = motifsRes.odags
   assert (odags.count != 0)
   val embeddings = motifsRes.embeddings
   assert (embeddings.count == motifsNumEmbeddings)
   assert (embeddings.distinct.count == motifsNumEmbeddings)
 }
 test ("[motifs,embedding] arabesque API") {
   val motifsRes = arabGraph.motifs (3).
     set ("comm_strategy", COMM_EMBEDDING)
   val odags = motifsRes.odags
   assert (odags.count == 0)
   val embeddings = motifsRes.embeddings
   assert (embeddings.count == motifsNumEmbeddings)
   assert (embeddings.distinct.count == motifsNumEmbeddings)
 }
 test ("[motifs,custom computation equivalence] arabesque API") {
   import org.apache.hadoop.io.LongWritable
   import io.arabesque.pattern.Pattern
   import io.arabesque.utils.SerializableWritable
   import io.arabesque.aggregation.reductions.LongSumReduction

   val AGG_MOTIFS = "motifs"
   val motifsRes: ArabesqueResult[_] = arabGraph.
     vertexInducedComputation (
       new VertexProcessFunc {
         private lazy val longUnit = new LongWritable(1)
         def apply(e: VertexInducedEmbedding, c: Computation[VertexInducedEmbedding]): Unit = {
           if (e.getNumWords == 3) {
             c.output (e)
             c.map (AGG_MOTIFS, e.getPattern, longUnit)
           }
         }
       }
     ).
     withShouldExpand ((e,c) => e.getNumVertices < 3).
     withAggregation [Pattern,LongWritable] (AGG_MOTIFS)(
       (v1, v2) => {v1.set (v1.get + v2.get); v1})

   val odags = motifsRes.odags
   assert (odags.count != 0)
   val embeddings = motifsRes.embeddings
   assert (embeddings.count == motifsNumEmbeddings)
   assert (embeddings.distinct.count == motifsNumEmbeddings)

 }

 val fsmNumEmbeddings = 31414
 test ("[fsm,odag] arabesque API") {
   val fsmRes = arabGraph.fsm (100, 3).
     set ("comm_strategy", COMM_ODAG_SP)
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
 test ("[fsm,custom computation equivalence] arabesque API") {
   import io.arabesque.gmlib.fsm._
   import io.arabesque.pattern.Pattern
   import io.arabesque.utils.SerializableWritable
   import java.lang.ThreadLocal

   val AGG_SUPPORT = "support"
   val support = 100
   val maxSize = 3
   val fsmRes = arabGraph.
     edgeInducedComputation { new EdgeProcessFunc {
       lazy val domainSupport = new ThreadLocal [DomainSupport] {
         override def initialValue = new DomainSupport(support)
       }
       def apply (e: EdgeInducedEmbedding, c: Computation[EdgeInducedEmbedding]): Unit = {
         domainSupport.get.setFromEmbedding (e)
         c.map(AGG_SUPPORT, e.getPattern, domainSupport.get)
       }
     }}.
     withShouldExpand ((e,c) => e.getNumWords < maxSize).
     withPatternAggregationFilter ((p,c) => c.readAggregation(AGG_SUPPORT).containsKey (p)).
     withAggregationProcess ((e,c) => c.output (e)).
     withMasterCompute { c =>
       if (c.readAggregation (AGG_SUPPORT).getNumberMappings <= 0 && c.getStep > 0) {
         c.haltComputation()
       }
     }.
     withAggregation [Pattern,DomainSupport] (AGG_SUPPORT,
       new DomainSupportReducer(), endAggregationFunction = new DomainSupportEndAggregationFunction())
   
   val odags = fsmRes.odags
   val embeddings = fsmRes.embeddings
   assert (embeddings.count == fsmNumEmbeddings)
   assert (embeddings.distinct.count == fsmNumEmbeddings)

 }

 val trianglesNumEmbeddings = 0
 test ("[triangles,odag] arabesque API") {
   val trianglesRes = arabGraph.triangles().
     set ("comm_strategy", COMM_ODAG_SP)
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
 test ("[triangles,custom computation equivalence] arabesque API") {
   import org.apache.hadoop.io.{IntWritable, LongWritable}
   import io.arabesque.utils.SerializableWritable
   import io.arabesque.aggregation.reductions.LongSumReduction
   import io.arabesque.utils.collection.IntArrayList

   val longUnitSer = new SerializableWritable (new LongWritable(1))
   val AGG_OUTPUT = "output"
   val trianglesRes = arabGraph.
     vertexInducedComputation { (e,c) =>
       if (e.getNumVertices == 3) {
         val vertices = e.getVertices
         val id = new IntWritable()
         var i = 0
         while (i < 3) {
           id.set (vertices.getUnchecked(i))
           c.map (AGG_OUTPUT, id, longUnitSer.value)
           i += 1
         }
       }
     }.
     withShouldExpand ((e,c) => e.getNumVertices < 3).
     withFilter ((e,c) => e.getNumVertices < 3 ||
       (e.getNumVertices == 3 && e.getNumEdges == 3)).
     withAggregation [IntWritable,LongWritable] (AGG_OUTPUT, new LongSumReduction)

   val odags = trianglesRes.odags
   val embeddings = trianglesRes.embeddings
   assert (embeddings.count == trianglesNumEmbeddings)
   assert (embeddings.distinct.count == trianglesNumEmbeddings)
 }

 val cliquesNumEmbeddings = 1166
 test ("[cliques,odag] arabesque API") {
   val cliquesRes = arabGraph.cliques (3).
     set ("comm_strategy", COMM_ODAG_SP)
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
 test ("[cliques,custom computation equivalence] arabesque API") {
   val maxsize = 3
   val cliquesRes = arabGraph.
     vertexInducedComputation { (e,c) =>
       if (e.getNumVertices == maxsize) {
         c.output (e)
       }
     }.
     withShouldExpand ((e,c) => e.getNumVertices < maxsize).
     withFilter ((e,c) => e.getNumEdgesAddedWithExpansion == e.getNumVertices - 1)

   val odags = cliquesRes.odags
   val embeddings = cliquesRes.embeddings
   assert (embeddings.count == cliquesNumEmbeddings)
   assert (embeddings.distinct.count == cliquesNumEmbeddings)
 }
 
 test ("[cliques percolation] arabesque API") {
   import io.arabesque.utils.collection.{IntArrayList, UnionFindOps}
   import scala.collection.JavaConverters._
   import org.apache.hadoop.io._
   val maxsize = 3
   val cliquepercRes = arabGraph.cliquesPercolation (maxsize)
   val cliqueAdjacencies = cliquepercRes.aggregationStorage [IntArrayList,IntArrayList] ("membership")
   val cliqueAdjacenciesBc = sc.broadcast (cliqueAdjacencies)
   val cliques = cliquepercRes.aggregationRDD [IntArrayList,VertexInducedEmbedding] ("cliques")
   val communities = cliques.map { case (repr,e) =>
     val m = cliqueAdjacenciesBc.value
     val key = UnionFindOps.find [IntArrayList] (
       v => m.getValue(v),
       (k,v) => m.aggregateWithReusables (k, v),
       repr.value
     )
     (key, e.value)
   }.reduceByKey { (e1,e2) =>
     e2.getVertices.iterator.asScala.foreach (v => if (!(e1.getVertices contains v)) e1.addWord (v))
     e1
   }

   assert (communities.count == 234)
 }
} 
