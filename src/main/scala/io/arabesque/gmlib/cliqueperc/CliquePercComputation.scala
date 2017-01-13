package io.arabesque.gmlib.cliqueperc

import io.arabesque.aggregation.AggregationStorage
import io.arabesque.aggregation.reductions.ReductionFunctionContainer
import io.arabesque.computation.VertexInducedComputation
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding.{Embedding, VertexInducedEmbedding}
import io.arabesque.graph.BasicMainGraph
import io.arabesque.utils.collection.{IntArrayList, UnionFindOps}

import org.apache.hadoop.io.IntWritable

import scala.collection.JavaConverters._

class CliquePercComputation extends VertexInducedComputation[VertexInducedEmbedding] {
  import CliquePercComputation._

  private var maxsize: Int = _
  private lazy val membershipAggStorage: CliquePercAggregationStorage =
    getAggregationStorage (MEMBERSHIP).asInstanceOf[CliquePercAggregationStorage]
  private val reusableInt: IntWritable = new IntWritable()
  private val updateCallback = 
    (k: IntArrayList, v: IntArrayList) => membershipAggStorage.aggregateWithReusables(k,v)
  private val queryCallback = 
    (v: IntArrayList) => membershipAggStorage.getValue(v)

  override def init(): Unit = {
    super.init()
    maxsize = SparkConfiguration.get.getInteger (MAXSIZE, MAXSIZE_DEFAULT)
  }

  override def initAggregations(): Unit = {
    super.initAggregations()
    val conf = SparkConfiguration.get

    // aggregation for clique adjacencies.
    // Key: vertice;
    // Value: parent representing the set the vertice belongs according to an union-find structure
    conf.registerAggregation(MEMBERSHIP, classOf[CliquePercAggregationStorage], classOf[IntArrayList],
      classOf[IntArrayList], false,
      new ReductionFunctionContainer [IntArrayList] ((k1, k2) => {k1.set (k2); k1})
    );

    // aggregation for cliques, that will compose a community.
    // Key: any vertice from the embedding to represent the clique (we choose to
    // take the first)
    // Value: embedding representing a clique
    val embeddingReduce = (e1: VertexInducedEmbedding, e2: VertexInducedEmbedding) => {
      val wordsIter = e2.getWords.iterator
      while (wordsIter.hasNext) {
        val w = wordsIter.next
        if (!e1.getWords.contains(w)) {
          e1.addWord (w);
        }
      }
      e1
    }
    conf.registerAggregation(CLIQUES, classOf[IntArrayList],
      classOf[VertexInducedEmbedding], false, new ReductionFunctionContainer(embeddingReduce))
  }
  
  private def isClique(e: VertexInducedEmbedding): Boolean = {
    e.getNumEdgesAddedWithExpansion() == e.getNumVertices() - 1
  }

  override def filter(e: VertexInducedEmbedding): Boolean = {
    isClique (e)
  }

  override def shouldExpand(e: VertexInducedEmbedding): Boolean = {
    e.getNumVertices < maxsize
  }

  override def process(e: VertexInducedEmbedding): Unit = {
    if (e.getNumVertices == maxsize) { // process only cliques
      val subsetsIter = e.getVertices.combinations (maxsize - 1)
      val repr = UnionFindOps.find (queryCallback, updateCallback, new IntArrayList(subsetsIter.next))
      map (CLIQUES, repr, e)
      while (subsetsIter.hasNext) {
        val subset = subsetsIter.next
        UnionFindOps.union (queryCallback, updateCallback, subset, repr)
      }
    }
  }
}

object CliquePercComputation {
  val MAXSIZE = "arabesque.clique.maxsize"
  val MAXSIZE_DEFAULT = 4

  val MEMBERSHIP = "membership"
  val CLIQUES = "cliques"
}
