package io.arabesque.report

/**
  * Created by ehussein on 7/31/17.
  */
trait Report {
  def saveReport(path: String) : Unit
}
