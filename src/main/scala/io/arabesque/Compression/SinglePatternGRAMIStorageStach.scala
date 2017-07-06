package io.arabesque.Compression

import io.arabesque.computation.Computation
import io.arabesque.conf.Configuration
import io.arabesque.embedding.Embedding
import io.arabesque.odag.domain.StorageReader
import io.arabesque.odag.domain.StorageStats
import io.arabesque.pattern.Pattern
import io.arabesque.odag.BasicODAGStash.EfficientReader
import org.apache.giraph.aggregators.BasicAggregator
import org.apache.log4j.Logger
import java.io._
import java.util
import java.util._
import java.util.concurrent.ExecutorService

import io.arabesque.odag.SinglePatternODAG


/**
  * Created by ehussein on 7/6/17.
  */
abstract class SinglePatternGRAMIStorageStach extends BasicGRAMIStorageStach with Externalizable {
  private val LOG = Logger.getLogger(classOf[SinglePatternGRAMIStorageStach])
  private var compressedEmbeddingsByPattern: Map[Pattern, SinglePatternGRAMIStorage] = null //new util.HashMap[Pattern, SinglePatternGRAMIStorage]()
  private var reusablePattern: Pattern = null

  def this() = {
    this
    this.compressedEmbeddingsByPattern = new util.HashMap[Pattern, SinglePatternGRAMIStorage]()
  }

  def this(storageByPattern: Map[Pattern, SinglePatternGRAMIStorage]) = {
    this()
    this.compressedEmbeddingsByPattern = storageByPattern
    this.reusablePattern = Configuration.get().createPattern()
  }


}
