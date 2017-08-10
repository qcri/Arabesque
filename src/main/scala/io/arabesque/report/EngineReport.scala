package io.arabesque.report

/**
  * Created by ehussein on 7/31/17.
  */
abstract class EngineReport extends Report {
  var superstep: Int = 0
  var startTime: Long = 0
  var endTime: Long = 0

  def getRuntime: Long = endTime - startTime
}
