package io.arabesque.report

import scala.collection.mutable.ArrayBuffer
import java.io._

/**
  * Created by ehussein on 7/31/17.
  */
class MasterReport extends EngineReport {
  var storageSummary: ArrayBuffer[String] = new ArrayBuffer[String]()
  var storageSize: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  var patternSize: ArrayBuffer[Long] = new ArrayBuffer[Long]()

  override def saveReport(path: String) = {
    val filePath = s"$path/MasterReport_SuperStep$superstep.txt"
    val pw: PrintWriter = new PrintWriter(new File(filePath))

    pw.println(s"SuperStep_runtime=$getRuntime")

    var i = 0
    while(i < storageSize.length) {
      pw.println(s"\nStorage$i=${storageSummary(i)}")
      pw.println(s"Storage${i}Size(bytes)=${storageSize(i)}")
      pw.println(s"Pattern${i}Size(bytes)=${patternSize(i)}")
      i += 1
    }

    pw.close()
  }
}
