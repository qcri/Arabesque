package io.arabesque.computation

import io.arabesque.embedding._
import io.arabesque.pattern.Pattern

sealed trait ComputationContainer [E <: Embedding] extends Computation[E] {

  val processOpt: Option[(E,Computation[E]) => Unit]
  val filterOpt: Option[(E,Computation[E]) => Boolean]
  val shouldExpandOpt: Option[(E,Computation[E]) => Boolean]
  val aggregationFilterOpt: Option[(E,Computation[E]) => Boolean]
  val pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean]
  val aggregationProcessOpt: Option[(E,Computation[E]) => Unit]
  val handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit]
  val initOpt: Option[(Computation[E]) => Unit]
  val initAggregationsOpt: Option[(Computation[E]) => Unit]
  val finishOpt: Option[(Computation[E]) => Unit]

  def withNewFunctions(
      processOpt: Option[(E,Computation[E]) => Unit] = processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] = filterOpt,
      shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = shouldExpandOpt,
      aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = aggregationFilterOpt,
      pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = pAggregationFilterOpt,
      aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = aggregationProcessOpt,
      handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = handleNoExpansionsOpt,
      initOpt: Option[(Computation[E]) => Unit] = initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] = initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] = finishOpt)
  : ComputationContainer[E]

  def shallowCopy(): ComputationContainer[E] = this.withNewFunctions()

}

case class EComputationContainer [E <: EdgeInducedEmbedding] (
    processOpt: Option[(E,Computation[E]) => Unit],
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = None,
    aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = None,
    pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = None,
    aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = None,
    handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None)
  extends EdgeInducedComputation[E] with ComputationContainer[E] {

  @transient private lazy val _process: (E,Computation[E]) => Unit = processOpt.get
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))

  @transient private lazy val _shouldExpand: (E,Computation[E]) => Boolean =
    shouldExpandOpt.getOrElse ((e: E, c: Computation[E]) => super.shouldExpand(e))
  
  @transient private lazy val _aggregationFilter: (E,Computation[E]) => Boolean =
    aggregationFilterOpt.getOrElse ((e: E, c: Computation[E]) => super.aggregationFilter (e))
  
  @transient private lazy val _pAggregationFilter: (Pattern,Computation[E]) => Boolean =
    pAggregationFilterOpt.getOrElse ((p: Pattern, c: Computation[E]) => super.aggregationFilter (p))
  
  @transient private lazy val _aggregationProcess: (E,Computation[E]) => Unit =
    aggregationProcessOpt.getOrElse ((e: E, c: Computation[E]) => super.aggregationProcess (e))
  
  @transient private lazy val _handleNoExpansions: (E,Computation[E]) => Unit =
    handleNoExpansionsOpt.getOrElse ((e: E, c: Computation[E]) => super.handleNoExpansions (e))
  
  @transient private lazy val _init: (Computation[E]) => Unit = initOpt match {
    case Some(thisInit) =>
      (c: Computation[E]) => {super.init(); thisInit(c)}
    case None =>
      (c: Computation[E]) => {super.init()}
  }

  @transient private lazy val _initAggregations: (Computation[E]) => Unit = initAggregationsOpt match {
    case Some(thisInitAggregations) =>
      (c: Computation[E]) => {super.initAggregations(); thisInitAggregations(c)}
    case None =>
      (c: Computation[E]) => {super.initAggregations()}
  }

  @transient private lazy val _finish: (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }

  def withNewFunctions(
      processOpt: Option[(E,Computation[E]) => Unit] = processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] = filterOpt,
      shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = shouldExpandOpt,
      aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = aggregationFilterOpt,
      pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = pAggregationFilterOpt,
      aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = aggregationProcessOpt,
      handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = handleNoExpansionsOpt,
      initOpt: Option[(Computation[E]) => Unit] = initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] = initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] = finishOpt)
    : ComputationContainer[E] = {
    this.copy (processOpt = processOpt, filterOpt = filterOpt,
      shouldExpandOpt = shouldExpandOpt, aggregationFilterOpt = aggregationFilterOpt,
      pAggregationFilterOpt = pAggregationFilterOpt, aggregationProcessOpt = aggregationProcessOpt,
      handleNoExpansionsOpt = handleNoExpansionsOpt,
      initOpt = initOpt, initAggregationsOpt = initAggregationsOpt, finishOpt = finishOpt)
  }

  override def process(e: E): Unit = _process (e, this)
  override def filter(e: E): Boolean = _filter (e, this)
  override def shouldExpand(e: E): Boolean = _shouldExpand (e, this)
  override def aggregationFilter(e: E): Boolean = _aggregationFilter (e, this)
  override def aggregationFilter(p: Pattern): Boolean = _pAggregationFilter (p, this)
  override def aggregationProcess(e: E): Unit = _aggregationProcess (e, this)
  override def handleNoExpansions(e: E): Unit = _handleNoExpansions (e, this)
  override def init(): Unit = _init (this)
  override def initAggregations(): Unit = _initAggregations (this)
  override def finish(): Unit = _finish (this)
}

case class VComputationContainer [E <: VertexInducedEmbedding] (
    processOpt: Option[(E,Computation[E]) => Unit],
    filterOpt: Option[(E,Computation[E]) => Boolean] = None,
    shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = None,
    aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = None,
    pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = None,
    aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = None,
    handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = None,
    initOpt: Option[(Computation[E]) => Unit] = None,
    initAggregationsOpt: Option[(Computation[E]) => Unit] = None,
    finishOpt: Option[(Computation[E]) => Unit] = None)
  extends VertexInducedComputation[E] with ComputationContainer[E] {

  @transient private lazy val _process: (E,Computation[E]) => Unit = processOpt.get
  
  @transient private lazy val _filter: (E,Computation[E]) => Boolean =
    filterOpt.getOrElse ((e: E, c: Computation[E]) => super.filter (e))

  @transient private lazy val _shouldExpand: (E,Computation[E]) => Boolean =
    shouldExpandOpt.getOrElse ((e: E, c: Computation[E]) => super.shouldExpand(e))
  
  @transient private lazy val _aggregationFilter: (E,Computation[E]) => Boolean =
    aggregationFilterOpt.getOrElse ((e: E, c: Computation[E]) => super.aggregationFilter (e))
  
  @transient private lazy val _pAggregationFilter: (Pattern,Computation[E]) => Boolean =
    pAggregationFilterOpt.getOrElse ((p: Pattern, c: Computation[E]) => super.aggregationFilter (p))
  
  @transient private lazy val _aggregationProcess: (E,Computation[E]) => Unit =
    aggregationProcessOpt.getOrElse ((e: E, c: Computation[E]) => super.aggregationProcess (e))
  
  @transient private lazy val _handleNoExpansions: (E,Computation[E]) => Unit =
    handleNoExpansionsOpt.getOrElse ((e: E, c: Computation[E]) => super.handleNoExpansions (e))
  
  @transient private lazy val _init: (Computation[E]) => Unit = initOpt match {
    case Some(thisInit) =>
      (c: Computation[E]) => {super.init(); thisInit(c)}
    case None =>
      (c: Computation[E]) => {super.init()}
  }

  @transient private lazy val _initAggregations: (Computation[E]) => Unit = initAggregationsOpt match {
    case Some(thisInitAggregations) =>
      (c: Computation[E]) => {super.initAggregations(); thisInitAggregations(c)}
    case None =>
      (c: Computation[E]) => {super.initAggregations()}
  }

  @transient private lazy val _finish: (Computation[E]) => Unit = finishOpt match {
    case Some(thisFinish) =>
      (c: Computation[E]) => {super.finish(); thisFinish(c)}
    case None =>
      (c: Computation[E]) => {super.finish()}
  }

  def withNewFunctions(
      processOpt: Option[(E,Computation[E]) => Unit] = processOpt,
      filterOpt: Option[(E,Computation[E]) => Boolean] = filterOpt,
      shouldExpandOpt: Option[(E,Computation[E]) => Boolean] = shouldExpandOpt,
      aggregationFilterOpt: Option[(E,Computation[E]) => Boolean] = aggregationFilterOpt,
      pAggregationFilterOpt: Option[(Pattern,Computation[E]) => Boolean] = pAggregationFilterOpt,
      aggregationProcessOpt: Option[(E,Computation[E]) => Unit] = aggregationProcessOpt,
      handleNoExpansionsOpt: Option[(E,Computation[E]) => Unit] = handleNoExpansionsOpt,
      initOpt: Option[(Computation[E]) => Unit] = initOpt,
      initAggregationsOpt: Option[(Computation[E]) => Unit] = initAggregationsOpt,
      finishOpt: Option[(Computation[E]) => Unit] = finishOpt)
    : ComputationContainer[E] = {
    this.copy (processOpt = processOpt, filterOpt = filterOpt,
      shouldExpandOpt = shouldExpandOpt, aggregationFilterOpt = aggregationFilterOpt,
      pAggregationFilterOpt = pAggregationFilterOpt, aggregationProcessOpt = aggregationProcessOpt,
      handleNoExpansionsOpt = handleNoExpansionsOpt,
      initOpt = initOpt, initAggregationsOpt = initAggregationsOpt, finishOpt = finishOpt)
  }

  override def process(e: E): Unit = _process (e, this)
  override def filter(e: E): Boolean = _filter (e, this)
  override def shouldExpand(e: E): Boolean = _shouldExpand (e, this)
  override def aggregationFilter(e: E): Boolean = _aggregationFilter (e, this)
  override def aggregationFilter(p: Pattern): Boolean = _pAggregationFilter (p, this)
  override def aggregationProcess(e: E): Unit = _aggregationProcess (e, this)
  override def handleNoExpansions(e: E): Unit = _handleNoExpansions (e, this)
  override def init(): Unit = _init (this)
  override def initAggregations(): Unit = _initAggregations (this)
  override def finish(): Unit = _finish (this)
}

object ComputationContainer {
}
