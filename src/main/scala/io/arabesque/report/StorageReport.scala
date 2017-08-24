package io.arabesque.report

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.StringBuilder

/**
  * Created by ehussein on 7/31/17.
  */
class StorageReport {
  // num of actual embeddings stored in the storage
  var numActualEmbeddings: Long = 0
  // Total number of enumerations the storage could hold
  var numEnumerations: Long = 0
  // used to show load balancing between partitions
  var numCompleteEnumerationsVisited: Long = 0
  // how many invalid embeddings this storage/partition generated
  var numSpuriousEmbeddings: Long = 0L

  var domainSize: ArrayBuffer[Int] = null // new ArrayBuffer[Int]()
  var explored: ArrayBuffer[Int] = null // new ArrayBuffer[Int]()
  var pruned: ArrayBuffer[Int] = null // new ArrayBuffer[Int]()
  var storageId: Int = 0

  // setters

  def setNumActualEmbeddings(value: Long): Unit = { this.numActualEmbeddings = value }
  def setNumEnumerations(value: Long): Unit = { this.numEnumerations = value }
  def setNumCompleteEnumerationsVisited(value: Long): Unit = { this.numCompleteEnumerationsVisited = value }
  def setNumSpuriousEmbeddings(value: Long): Unit = { this.numSpuriousEmbeddings = value }
  def setPruned(index: Int, value: Int): Unit = { this.pruned(index) = value }
  def setExplored(index: Int, value: Int): Unit = { this.explored(index) = value }
  def setDomainSize(index: Int, size: Int): Unit = { this.domainSize(index) = size }

  // getters
  def getNumActualEmbeddings: Long = this.numActualEmbeddings
  def getNumEnumerations: Long = this.numEnumerations
  def getNumCompleteEnumerationsVisited: Long = this.numCompleteEnumerationsVisited
  def getNumSpuriousEmbeddings: Long = this.numSpuriousEmbeddings
  def getPruned(index: Int): Int = this.pruned(index)
  def getExplored(index: Int): Int = this.explored(index)
  def getDomainSize(index: Int): Int = this.domainSize(index)

  def incrementPruned(index: Int, value: Int): Unit = {
    this.pruned(index) += value
  }

  def incrementExplored(index: Int, value: Int): Unit = {
    this.explored(index) += value
  }

  def initReport(numberOfDomains: Int): Unit = {
    pruned = ArrayBuffer.fill(numberOfDomains)(0)
    explored = ArrayBuffer.fill(numberOfDomains)(0)
    domainSize = ArrayBuffer.fill(numberOfDomains)(0)
  }

  override def toString(): String = toJSONString()

  def toNormalString(): String = {
    val str: StringBuilder = new mutable.StringBuilder()

    str.append(s"Storage${storageId}_Report: {")
    str.append(s"\nNumEnumerations=$numEnumerations")
    str.append(s"\nNumStoredEmbeddings=$numActualEmbeddings")
    str.append(s"\nNumCompleteEnumerationsVisited=$numCompleteEnumerationsVisited")
    str.append(s"\nNumSpuriousEmbeddings=$numSpuriousEmbeddings")
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

  def toJSONString(): String = {
    val str: StringBuilder = new mutable.StringBuilder()
    val domainSizeStr: StringBuilder = new mutable.StringBuilder()
    val prunedStr: StringBuilder = new mutable.StringBuilder()
    val exploredStr: StringBuilder = new mutable.StringBuilder()

    str.append(s"""{\"StorageID\":$storageId, """)
    str.append(s"""\"NumEnumerations\":$numEnumerations, """)
    str.append(s"""\"NumStoredEmbeddings\":$numActualEmbeddings, """)
    str.append(s"""\"NumCompleteEnumerationsVisited\":$numCompleteEnumerationsVisited, """)
    str.append(s"""\"NumSpuriousEmbeddings\":$numSpuriousEmbeddings, """)

    domainSizeStr.append("\"DomainSize\":[")
    prunedStr.append("\"Pruned\":[")
    exploredStr.append("\"Explored\":[")

    var i = 0
    while(i < domainSize.size) {
      domainSizeStr.append(domainSize(i))
      prunedStr.append(pruned(i))
      exploredStr.append(explored(i))

      i += 1
      if(i != domainSize.size) {
        domainSizeStr.append(", ")
        prunedStr.append(", ")
        exploredStr.append(", ")
      }
    }
    domainSizeStr.append("]")
    prunedStr.append("]")
    exploredStr.append("]")

    str.append(domainSizeStr + ", ")
    str.append(prunedStr + ", ")
    str.append(exploredStr)
    str.append("}")

    str.toString()
  }
}
