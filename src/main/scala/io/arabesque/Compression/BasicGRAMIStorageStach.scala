package io.arabesque.Compression

import java.util
import java.util.concurrent.ExecutorService

import io.arabesque.computation.Computation
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.StorageReader
import org.apache.hadoop.io.Writable

/**
  * Created by ehussein on 7/5/17.
  */

abstract class BasicGRAMIStorageStach[O <: BasicGRAMIStorage, S <: BasicGRAMIStorageStach[O,S]] extends Writable {

  trait Reader[E <: Embedding] extends util.Iterator[E] {}

  class EfficientReader[E <: Embedding] extends Reader[E] {
    private var numPartitions: Int = _
    private var numBlocks: Int = _
    private var maxBlockSize: Int = _
    private var computation: Computation[Embedding] = _
    private var stashIterator: util.Iterator[_ <: BasicGRAMIStorage] = _
    private var currentReader: StorageReader = _
    private var currentPositionConsumed: Boolean = true

    def this(stash: BasicGRAMIStorageStach[_ <: BasicGRAMIStorage, _], computation: Computation[_ <: Embedding], numPartitions: Int, numBlocks: Int, maxBlockSize: Int) = {
      this()
      this.numPartitions = numPartitions
      this.computation = computation.asInstanceOf[Computation[Embedding]]
      this.numBlocks = numBlocks
      this.maxBlockSize = maxBlockSize
      stashIterator = stash.getEzips.iterator
      currentReader = null
    }

    override def hasNext: Boolean = {
      while (true) {
        if (currentReader == null) {
          if (stashIterator.hasNext)
            currentReader = stashIterator.next.getReader(computation, numPartitions, numBlocks, maxBlockSize)
        }

        // No more zips, for sure nothing else to do
        if (currentReader == null) {
          currentPositionConsumed = true
          return false
        }

        // If we consumed the current embedding (called next after a previous hasNext),
        // we need to actually advance to the next one.
        if (currentPositionConsumed && currentReader.hasNext) {
          currentPositionConsumed = false
          return true
        }
        // If we still haven't consumed the current embedding (called hasNext but haven't
        // called next), return the same result as before (which is necessarily true).
        else if (!currentPositionConsumed)
          return true
        // If we have consumed the current embedding and the current reader doesn't have
        // more embeddings, we need to advance to the next reader so set currentReader to
        // null and let the while begin again (simulate recursive call without the stack
        // building overhead).
        else {
          currentReader.close()
          currentReader = null
        }
      }
      /// needs to be checked again
      false
    }

    override def next: E = {
      currentPositionConsumed = true
      currentReader.next.asInstanceOf[E]
    }

    override def remove(): Unit = {
      throw new UnsupportedOperationException
    }
  }

  def addEmbedding(embedding: Embedding): Unit

  def aggregate(gramiStorage: O): Unit

  def aggregateUsingReusable(ezip: O): Unit

  def aggregateStash(value: S): Unit

  def finalizeConstruction(pool: ExecutorService, parts: Int): Unit

  def isEmpty: Boolean

  def getNumZips: Int

  def getEzips: util.Collection[O]

  def clear(): Unit
}