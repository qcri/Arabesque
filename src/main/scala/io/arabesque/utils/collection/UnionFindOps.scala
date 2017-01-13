package io.arabesque.utils.collection

import io.arabesque.aggregation.AggregationStorage
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding.VertexInducedEmbedding

import org.apache.hadoop.io._

import scala.collection.JavaConverters._

/**
 * This object implements the operations of an union-find structure.
 * TODO: implement union-find optimizations
 * ("path compression" and "union by rank")
 */
object UnionFindOps {

  /**
   * Returns a number representing the disjoint set that *i* belongs to.
   */
  @scala.annotation.tailrec
  def find[T <: Writable](queryCallback: (T) => T,
      updateCallback: (T,T) => Unit, i: T): T = {
    var parent = queryCallback(i)
    if (parent == null) {
      updateCallback (i, i)
      parent = i
    }

    if (parent equals i) i
    else find (queryCallback, updateCallback, parent)
  }

  /**
   * This functions does the union between the sets containing *i* and *j*,
   * respectively.
   */
  def union[T <: Writable](queryCallback: (T) => T,
      updateCallback: (T,T) => Unit, i: T, j: T): Unit = {
    val parenti = find (queryCallback, updateCallback, i)
    val parentj = find (queryCallback, updateCallback, j)
    updateCallback (parenti, parentj)
  }
}
