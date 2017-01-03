package io.arabesque.computation

import io.arabesque.embedding._
import io.arabesque.pattern.Pattern

case class MasterComputationContainer(
    initOpt: Option[(MasterComputation) => Unit] = None,
    computeOpt: Option[(MasterComputation) => Unit] = None)
  extends MasterComputation {

  @transient private lazy val _init: (MasterComputation) => Unit = initOpt match {
    case Some(thisInit) =>
      (c: MasterComputation) => thisInit (c)
    case None =>
      (c: MasterComputation) => {}
  }

  @transient private lazy val _compute: (MasterComputation) => Unit = computeOpt match {
    case Some(thisCompute) =>
      (c: MasterComputation) => thisCompute (c)
    case None =>
      (c: MasterComputation) => {}
  }

  def withNewFunctions(
      initOpt: Option[(MasterComputation) => Unit] = initOpt,
      computeOpt: Option[(MasterComputation) => Unit] = computeOpt)
    : MasterComputationContainer = {
    this.copy (initOpt = initOpt, computeOpt = computeOpt)
  }

  def shallowCopy(): MasterComputationContainer = this.withNewFunctions()

  override def init(): Unit = _init (this)
  override def compute(): Unit = _compute (this)

}
