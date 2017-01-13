package io.arabesque.gmlib.cliqueperc

import io.arabesque.aggregation.AggregationStorage
import io.arabesque.conf.SparkConfiguration
import io.arabesque.embedding.VertexInducedEmbedding
import io.arabesque.utils.collection.{IntArrayList, UnionFindOps}

import org.apache.hadoop.io.{ArrayPrimitiveWritable, IntWritable, NullWritable}

import scala.collection.JavaConverters._

class CliquePercAggregationStorage extends AggregationStorage[IntArrayList,IntArrayList] {

  /**
   * This aggregation must be overrided because we must perform the union of
   * different union-find subsets (not necessarily disjoint).
   */
  override def aggregate(os: AggregationStorage[IntArrayList,IntArrayList]): Unit = {
    for ((k,_) <- os.getMapping.asScala) {
      val parent = UnionFindOps.find [IntArrayList] (
        v => os.getValue (v), (k,v) => os.aggregateWithReusables (k,v), k)
      if (!(parent equals k)) {
        UnionFindOps.union [IntArrayList] (
          v => this.getValue (v), (k,v) => this.aggregateWithReusables (k,v), parent, k)
      }
    }
  }

}
