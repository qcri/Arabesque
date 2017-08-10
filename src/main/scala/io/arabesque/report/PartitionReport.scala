package io.arabesque.report

import scala.collection.mutable.ArrayBuffer
import java.io._

/**
  * Created by ehussein on 7/31/17.
  */
class PartitionReport extends EngineReport {
  var partitionId: Int = 0
  var storageReports: ArrayBuffer[StorageReport] = new ArrayBuffer[StorageReport]()

  override def saveReport(path: String) = {
    val filePath = s"$path/Partition${partitionId}Report_SuperStep$superstep.txt"
    val pw: PrintWriter = new PrintWriter(new File(filePath))

    pw.println(s"partition_runtime=$getRuntime")

    var i = 0
    storageReports.foreach(storage => {
      pw.println(s"Storage${i}Report=${storageReports(i)}")
      i += 1
    })

    pw.close()
  }
}
