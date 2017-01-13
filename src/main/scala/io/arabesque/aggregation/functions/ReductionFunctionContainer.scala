package io.arabesque.aggregation.reductions

import org.apache.hadoop.io.Writable

case class ReductionFunctionContainer[V <: Writable] (func: (V,V) => V)
    extends ReductionFunction[V] {
  def reduce (v1: V, v2: V): V = func (v1, v2)
}
