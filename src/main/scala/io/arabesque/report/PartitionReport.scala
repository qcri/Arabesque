package io.arabesque.report

import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import java.io._

import scala.collection.mutable

/**
  * Created by ehussein on 7/31/17.
  */
class PartitionReport extends EngineReport {
  var partitionId: Int = 0
  var storageReports: ArrayBuffer[StorageReport] = new ArrayBuffer[StorageReport]()

  override def saveReport(path: String) = {
    val filePath = s"$path/Partition${partitionId}Report.txt"

    if(superstep == 0) {
      val file = new File(filePath)
      if(file.exists())
        file.delete()
    }
    else {
      // to remove "]" written at last superstep
      val raf: RandomAccessFile = new RandomAccessFile(new File(filePath), "rw")
      raf.setLength(raf.length() - 1)
      raf.close()
    }

    val pw: PrintWriter = new PrintWriter(new BufferedWriter(new FileWriter(filePath,true)))

    if(superstep == 0) {
      pw.print("[")
    }
    else
        pw.print(",")

    val report = toString()

    pw.print(report + "\n")
    pw.print("]")

    pw.close()
  }

  override def toString(): String  = toJSONString()

  def toJSONString(): String = {
    val str: StringBuilder = new mutable.StringBuilder()

    str.append(s"""{\"super_step\":$superstep, """)
    str.append(s"""\"runtime\":$getRuntime, """)

    str.append("\"StorageReports\":[")
    var i = 0
    while(i < storageReports.length) {
      storageReports(i).storageId = i
      str.append(s"${storageReports(i)}")
      i += 1

      if(i != storageReports.size)
        str.append(", ")
    }

    str.append("]")
    str.append("}")

    str.toString()
  }
}
