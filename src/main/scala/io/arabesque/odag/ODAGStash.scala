package io.arabesque.odag

import io.arabesque.conf.{Configuration, SparkConfiguration}
import io.arabesque.embedding.Embedding

object ODAGStash {
  import Configuration._
  import SparkConfiguration._

  def apply [E <: Embedding, O <: BasicODAG, S <: BasicODAGStash[O,S]]
    (config: Configuration[E]): S = 
      config.getString(CONF_COMM_STRATEGY, CONF_COMM_STRATEGY_DEFAULT) match {
    case (COMM_ODAG_SP | COMM_ODAG_SP_PRIM | COMM_ODAG_SP_GEN) =>
      new SinglePatternODAGStash().asInstanceOf[S]
    case (COMM_ODAG_MP | COMM_ODAG_MP_PRIM | COMM_ODAG_MP_GEN) =>
      new MultiPatternODAGStash().asInstanceOf[S]
  }
}
