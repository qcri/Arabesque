package io.arabesque.report

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder

/**
  * Created by ehussein on 7/31/17.
  */
class StorageReport {
  var numEnumerations: Long = 0
  var domainSize: ArrayBuffer[Int] = null // new ArrayBuffer[Int]()
  var explored: ArrayBuffer[Int] = new ArrayBuffer[Int]()
  var pruned: ArrayBuffer[Int] = null // new ArrayBuffer[Int]()
  var storageId: Int = 0

  override def toString(): String = {
    val str: StringBuilder = new mutable.StringBuilder()

    str.append(s"Storage${storageId}_Report: {")
    str.append(s"\nNumEnumerations=$numEnumerations")
    var i = 0
    while(i < domainSize.size) {
      str.append(s"\nDomain${i}Size=${domainSize(i)}, ")
      str.append(s"ExploredInDomain${i}=${explored(i)}, ")
      str.append(s"PrunedInDomain${i}=${pruned(i)}")
      i += 1
    }
    str.append("\n}")

    str.toString()
  }
}
