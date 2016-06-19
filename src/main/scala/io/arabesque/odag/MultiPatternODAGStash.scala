package io.arabesque.odag

import java.util.concurrent.ExecutorService
import java.io._

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.pattern.Pattern
import io.arabesque.conf.{Configuration, SparkConfiguration}

import scala.collection.JavaConverters._

class MultiPatternODAGStash
    extends BasicODAGStash[MultiPatternODAG,MultiPatternODAGStash] with Serializable {

  def config: SparkConfiguration[_ <: Embedding] =
    Configuration.get[SparkConfiguration[_ <: Embedding]]

  var odags: Array[MultiPatternODAG] = _
  var numOdags: Int = _
  @transient val reusablePattern: Pattern = config.createPattern

  def this(maxOdags: Int) = {
    this()
    odags = new Array(maxOdags)
    numOdags = 0
  }

  def this(odagMap: scala.collection.Map[_,MultiPatternODAG]) = {
    this()
    odags = odagMap.values.toArray
    numOdags = odags.size
  }

  def aggregationFilter(computation: Computation[_]): Unit = {
    for (i <- 0 until odags.size if odags(i) != null)
      if (!odags(i).aggregationFilter(computation)) {
        odags(i) = null
        numOdags -= 1
      }
  }

  override def addEmbedding(embedding: Embedding): Unit = {
    reusablePattern.setEmbedding (embedding)
    val idx = {
      val cand = reusablePattern.hashCode % getNumZips
      cand + (if (cand < 0) getNumZips else 0)
    }

    if (odags(idx) == null) {
      // initialize multi-pattern odag
      odags(idx) = new MultiPatternODAG (embedding.getNumWords)
      numOdags += 1
    }

    // add embedding to the selected odag
    odags(idx).addEmbedding (embedding, reusablePattern)
  }

  override def aggregate(odag: MultiPatternODAG): Unit = ???

  override def aggregateUsingReusable(odag: MultiPatternODAG): Unit = ???

  override def aggregateStash(other: MultiPatternODAGStash): Unit = {
  }
   
  override def finalizeConstruction(
      pool: ExecutorService, parts: Int): Unit = {
    assert (odags.size <= config.getMaxOdags)
    for (odag <- odags.iterator if odag != null)
      odag.finalizeConstruction (pool, parts)
  }

  override def isEmpty(): Boolean = odags.isEmpty

  override def getNumZips(): Int =
    if (odags != null) odags.size else 0

  override def getEzips() =
    odags.filter(_ != null).toIterable.asJavaCollection

  override def clear(): Unit = {
    for (i <- 0 until odags.size) odags(i) = null
  }

  //override def readExternal(objInput: ObjectInput): Unit = {
  //  readFields (objInput)
  //}

  //override def writeExternal(objOutput: ObjectOutput): Unit = {
  //  write (objOutput)
  //}
  
  override def readFields(dataInput: DataInput): Unit = ???
  override def write(dataOutput: DataOutput): Unit = ???
  
  //override def readExternal(objInput: ObjectInput): Unit = {
  //  odags = new Array(objInput.readInt)
  //  numOdags = objInput.readInt
  //  var readOdags = 0
  //  for (i <- 0 until numOdags) {
  //    val odag = new MultiPatternODAG
  //    odag.readExternal (objInput)
  //    odags(odag.patterns.head.hashCode % odags.size) = odag
  //    readOdags += 1
  //  }
  //  assert (odags.filter(_ != null).size == readOdags)
  //  assert (numOdags == readOdags)
  //}

  //override def writeExternal(objOutput: ObjectOutput): Unit = {
  //  objOutput.writeInt (odags.size)
  //  objOutput.writeInt (numOdags)
  //  var writtenOdags = 0
  //  odags.foreach {
  //    case odag if odag != null =>
  //      odag.writeExternal (objOutput)
  //      writtenOdags += 1
  //    case _ =>
  //  }
  //  assert (odags.filter(_ != null).size == writtenOdags)
  //  assert (writtenOdags == numOdags)
  //}
}

object MultiPatternODAGStash {
  def apply() = new MultiPatternODAGStash
}
