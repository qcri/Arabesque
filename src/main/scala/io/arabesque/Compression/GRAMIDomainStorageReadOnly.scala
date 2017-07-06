package io.arabesque.Compression

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.koloboke.collect.IntCollection
import com.koloboke.collect.set.hash.{HashIntSet, HashIntSets}
import io.arabesque.computation.Computation
import io.arabesque.conf.Configuration
import io.arabesque.embedding.{EdgeInducedEmbedding, Embedding, VertexInducedEmbedding}
import io.arabesque.graph.{Edge, LabelledEdge, MainGraph}
import io.arabesque.odag.domain.{DomainEntry, DomainEntryReadOnly, StorageReader}
import io.arabesque.pattern.{LabelledPatternEdge, Pattern, PatternEdge, PatternEdgeArrayList}
import io.arabesque.utils.collection.{IntArrayList, IntCollectionAddConsumer}
import org.apache.log4j.Logger

import scala.util.control.Breaks._

/**
  * Created by ehussein on 6/27/17.
  */
class GRAMIDomainStorageReadOnly extends GRAMIDomainStorage {
  private val LOG = Logger.getLogger(classOf[GRAMIDomainStorageReadOnly])

  @Override
  @throws(classOf[RuntimeException])
  override def getReader(pattern: Pattern, computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    new Reader(pattern, computation, numPartitions, numBlocks, maxBlockSize)
  }

  @Override
  @throws(classOf[RuntimeException])
  override def getReader(patterns: Array[Pattern], computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    //new MultiPatternReader(patterns, computation, numPartitions, numBlocks, maxBlockSize)
    throw new RuntimeException("Shouldn't use multi-pattern with GRAMI")
  }

  class Reader(val pattern: Pattern, val computation: Computation[Embedding], val numPartitions: Int, val numBlocks: Int, val maxBlockSize: Int) extends StorageReader {
    mainGraph = Configuration.get().getMainGraph()
    reusableEmbedding = Configuration.get().createEmbedding()
    this.numberOfEnumerations = getNumberOfEnumerations()
    this.blockSize = Math.min(Math.max(numberOfEnumerations / numBlocks, 1L), maxBlockSize)
    enumerationStack = new util.ArrayDeque
    enumerationStack.add(new Domain0EnumerationStep(0, -1, -1))
    singletonExtensionSet = HashIntSets.newMutableSet(1)
    partitionId = computation.getPartitionId
    targetEnumId = -1
    edgeIds = new IntArrayList
    edgesConsumer = new EdgesConsumer(Configuration.get().isGraphEdgeLabelled)
    edgesConsumer.setCollection(edgeIds)
    final private var mainGraph: MainGraph = null
    final private var reusableEmbedding: Embedding = null
    final private var numberOfEnumerations: Long = 0L
    final private var blockSize: Long = 0L
    final private var partitionId: Int = 0
    final private var enumerationStack: util.Deque[EnumerationStep] = null
    final private var singletonExtensionSet: HashIntSet = null
    private var targetEnumId: Long = 0L
    private var edgesConsumer: EdgesConsumer = null
    private var edgeIds: IntArrayList = null

    override def hasNext: Boolean = moveNext

    override def next: Embedding = reusableEmbedding

    override def remove(): Unit = {
      throw new UnsupportedOperationException
    }

    private def tryAddWord(wordId: Int): Boolean = {
      if (reusableEmbedding.isInstanceOf[VertexInducedEmbedding]) {
        val reusableVertexEmbedding: VertexInducedEmbedding = reusableEmbedding.asInstanceOf[VertexInducedEmbedding]
        val numVertices = reusableVertexEmbedding.getNumVertices
        val vertices: IntArrayList = reusableVertexEmbedding.getVertices
        // now let's check if the word has already been added to the embedding
        if (vertices.contains(wordId))
          return false
        singletonExtensionSet.clear()
        singletonExtensionSet.add(wordId)
        computation.filter(reusableEmbedding, singletonExtensionSet)
        if (singletonExtensionSet.size == 0)
          return false
        if (!computation.filter(reusableVertexEmbedding, wordId))
          return false
        reusableVertexEmbedding.addWord(wordId)
      }
      else if (reusableEmbedding.isInstanceOf[EdgeInducedEmbedding]) {
        val reusableEdgeEmbedding: EdgeInducedEmbedding = reusableEmbedding.asInstanceOf[EdgeInducedEmbedding]
        singletonExtensionSet.clear()
        singletonExtensionSet.add(wordId)
        computation.filter(reusableEmbedding, singletonExtensionSet)
        val edges: PatternEdgeArrayList = pattern.getEdges
        val equivalentPatternEdge: PatternEdge = edges.get(reusableEdgeEmbedding.getNumWords)
        val equivalentPatternEdgeSrcIndex: Int = equivalentPatternEdge.getSrcPos
        val equivalentPatternEdgeDestIndex: Int = equivalentPatternEdge.getDestPos
        reusableEdgeEmbedding.addWord(wordId)
        val embeddingVertices: IntArrayList = reusableEdgeEmbedding.getVertices
        val numEmbeddingVertices: Int = reusableEdgeEmbedding.getNumVertices
        reusableEdgeEmbedding.removeLastWord()
        // If pattern has more vertices than embedding with this word, quit,
        // expansion not valid
        if (equivalentPatternEdgeSrcIndex >= numEmbeddingVertices || equivalentPatternEdgeDestIndex >= numEmbeddingVertices)
          return false
        // Otherwise, if same number of vertices, check if the edge connecting the
        // vertices mapped from the pattern is the same that we are trying to add.
        // If not, quit, expansion not valid.
        val edgeIds: IntCollection = getEdgeIds(embeddingVertices.getUnchecked(equivalentPatternEdgeSrcIndex), embeddingVertices.getUnchecked(equivalentPatternEdgeDestIndex), equivalentPatternEdge)
        // NOTE: IntSet would theoretically allow faster contains but, in practice,
        // we assume not a lot of edges between 2 vertices exist with the same label
        // so array should be quicker.
        if (!edgeIds.contains(wordId))
          return false
        if (!computation.filter(reusableEdgeEmbedding, wordId))
          return false
        reusableEdgeEmbedding.addWord(wordId)
      }
      else
        throw new RuntimeException("Incompatible embedding class: " + reusableEmbedding.getClass)
      true
    }

    private class EdgesConsumer(var hasLabel: Boolean) extends IntCollectionAddConsumer {
      private var targetLabel: Int = 0

      def setPatternEdge(patternEdge: PatternEdge): Unit = {
        if (hasLabel)
          this.targetLabel = patternEdge.asInstanceOf[LabelledPatternEdge].getLabel
      }

      override def accept(edgeId: Int): Unit = {
        if (hasLabel) {
          val labelledEdge: LabelledEdge = mainGraph.getEdge(edgeId).asInstanceOf[LabelledEdge]
          if (labelledEdge.getEdgeLabel != targetLabel)
            return
        }
        super.accept(edgeId)
      }
    }

    private def getEdgeIds(srcId: Int, dstId: Int, patternEdge: PatternEdge): IntCollection = {
      edgeIds.clear()
      edgesConsumer.setPatternEdge(patternEdge)
      mainGraph.forEachEdgeId(srcId, dstId, edgesConsumer)
      edgeIds
    }

    private def testCompleteEmbedding: Boolean = {
      if (reusableEmbedding.getNumVertices != pattern.getNumberOfVertices)
        return false
      if (reusableEmbedding.isInstanceOf[VertexInducedEmbedding]) {
        val reusableVertexEmbedding: VertexInducedEmbedding = reusableEmbedding.asInstanceOf[VertexInducedEmbedding]
        // Test if constructed embedding matches the pattern.
        // TODO: Perhaps we can do this incrementally in an efficient manner?
        val numEdgesPattern: Int = pattern.getNumberOfEdges
        val numEdgesEmbedding: Int = reusableVertexEmbedding.getNumEdges
        if (numEdgesEmbedding != numEdgesPattern) return false
        val edgesPattern: PatternEdgeArrayList = pattern.getEdges
        val edgesEmbedding: IntArrayList = reusableVertexEmbedding.getEdges
        val verticesEmbedding: IntArrayList = reusableVertexEmbedding.getVertices
        var i = 0
        while (i < numEdgesPattern) {
          val edgePattern: PatternEdge = edgesPattern.get(i)
          val edgeEmbedding: Edge = mainGraph.getEdge(edgesEmbedding.getUnchecked(i))
          if (!edgeEmbedding.hasVertex(verticesEmbedding.getUnchecked(edgePattern.getSrcPos))
            || !edgeEmbedding.hasVertex(verticesEmbedding.getUnchecked(edgePattern.getDestPos)))
            return false

          i += 1
        }
      }
      computation.filter(reusableEmbedding) && computation.shouldExpand(reusableEmbedding)
    }

    def getEnumerationWithStack(targetSize: Int): Boolean = {
      var currentId: Long = 0
      while (!enumerationStack.isEmpty && targetEnumId >= currentId) {
        val lastEnumerationStep: EnumerationStep = enumerationStack.pop
        val domainOfLastEnumerationStep: Int = enumerationStack.size
        val wordIdOfLastEnumerationStep: Int = lastEnumerationStep.wordId
        currentId = lastEnumerationStep.currentId
        if (wordIdOfLastEnumerationStep >= 0) {
          currentId += domainEntries(domainOfLastEnumerationStep).get(wordIdOfLastEnumerationStep).getCounter
          reusableEmbedding.removeLastWord()
        }
        val domainWithPointers: Int = enumerationStack.size - 1
        // Need to increment index of first domain
        if (domainWithPointers == -1) {
          val domain0EnumerationStep: Domain0EnumerationStep = lastEnumerationStep.asInstanceOf[Domain0EnumerationStep]
          var currentIndex: Int = domain0EnumerationStep.index
          val domain0: util.Set[Int] = domainEntries(0)//.get(0)
          while ( {
            {
              currentIndex += 1;
              currentIndex
            } < domain0OrderedKeys.length
          }) {
            val wordId: Int = domain0OrderedKeys(currentIndex)
            val newPossibilityForDomain0: DomainEntry = domain0.get(wordId)
            if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForDomain0.getCounter > targetEnumId) || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
              var invalid: Boolean = false
              // If we couldn't add this word this means that the
              // current partial embedding and all extensions are invalid
              // so skip everything and return false since enumId was associated
              // with an invalid embedding.
              if (!tryAddWord(wordId)) {
                targetEnumId = currentId + newPossibilityForDomain0.getCounter - 1
                invalid = true
                // Add word anyway. Embedding will be invalid with this word but it will be
                // popped on the next iteration of the while
                reusableEmbedding.addWord(wordId)
              }
              domain0EnumerationStep.index = currentIndex
              domain0EnumerationStep.currentId = currentId
              domain0EnumerationStep.wordId = wordId
              enumerationStack.push(domain0EnumerationStep)
              if (invalid) return false
              else {
                if (enumerationStack.size != targetSize) {
                  val oneee: DomainEntryReadOnly = newPossibilityForDomain0.asInstanceOf[DomainEntryReadOnly]
                  enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers))
                }
                break //todo: break is not supported
              }
            }
            currentId += newPossibilityForDomain0.getCounter
          }
        }
        else {
          val domainNot0EnumerationStep: DomainNot0EnumerationStep = lastEnumerationStep.asInstanceOf[DomainNot0EnumerationStep]
          val possibilitiesLastDomain: ConcurrentHashMap[Integer, DomainEntry] = domainEntries.get(domainOfLastEnumerationStep)
          val pointers: Array[Int] = domainNot0EnumerationStep.domain
          var i: Int = domainNot0EnumerationStep.pos + 1
          while ( {
            i < pointers.length
          }) {
            val newWordId: Int = pointers(i)
            val newPossibilityForLastDomain: DomainEntry = possibilitiesLastDomain.get(newWordId)
            assert(newPossibilityForLastDomain != null)
            if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForLastDomain.getCounter > targetEnumId) || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
              var invalid: Boolean = false
              if (!tryAddWord(newWordId)) {
                targetEnumId = currentId + newPossibilityForLastDomain.getCounter - 1
                invalid = true
                reusableEmbedding.addWord(newWordId)
              }
              lastEnumerationStep.currentId = currentId
              lastEnumerationStep.wordId = newWordId
              lastEnumerationStep.asInstanceOf[DomainNot0EnumerationStep].pos = i
              enumerationStack.push(lastEnumerationStep)
              if (invalid) return false
              else {
                if (enumerationStack.size != targetSize) {
                  val oneee: DomainEntryReadOnly = newPossibilityForLastDomain.asInstanceOf[DomainEntryReadOnly]
                  enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers))
                }
                break //todo: break is not supported
              }
            }
            currentId += newPossibilityForLastDomain.getCounter

            {
              i += 1;
              i - 1
            }
          }
        }
        // If enumeration stack is of the desired size
        if (enumerationStack.size == targetSize) { // And last element actually represents a valid element
          if (enumerationStack.peek.wordId >= 0) { // Get out of the loop
            break //todo: break is not supported
          }
        }
      }
      reusableEmbedding.getNumWords == targetSize && testCompleteEmbedding
    }

    def toStringResume: String = {
      val sb: StringBuilder = new StringBuilder
      sb.append("EmbeddingsZip Reader:\n")
      sb.append("Enumerations: " + targetEnumId + " " + numberOfEnumerations + "\n")
      sb.toString
    }

    def moveNext: Boolean = {
      while (true) {
        targetEnumId = getNextEnumerationId(targetEnumId)
        if (targetEnumId == -1) return false
        if (getEnumerationWithStack(domainEntries.size)) return true
      }
    }

    def getNextEnumerationId(enumId: Long): Long = {
      while ( {
        enumId < numberOfEnumerations - 1
      }) {
        enumId += 1
        val currentBlockId: Long = enumId / blockSize
        if (isThisMyBlock(currentBlockId)) return enumId
        else { // -1 because we'll increment it at the beginning of the next iteration
          enumId = (currentBlockId + blocksToSkip(currentBlockId)) * blockSize - 1
        }
      }
      if (enumId >= numberOfEnumerations - 1) enumId = -1
      enumId
    }

    def blocksToSkip(blockId: Long): Int = {
      val owningPartition: Int = (blockId % numPartitions).toInt
      var myPartition: Int = partitionId
      if (myPartition < owningPartition) myPartition += numPartitions
      myPartition - owningPartition
    }

    def isThisMyBlock(blockId: Long): Boolean = blockId % numPartitions == partitionId

    override def close(): Unit = {
      // Do nothing by default
    }

    abstract class EnumerationStep(var currentId: Long, var wordId: Int) {
      override def toString: String = "EnumerationStep{" + "currentId=" + currentId + ", wordId=" + wordId + '}'
    }

    class Domain0EnumerationStep(override val currentId: Long,override val wordId: Int, var index: Int) extends EnumerationStep(currentId, wordId) {
      override def toString: String = "Domain0EnumerationStep{" + "index=" + index + "} " + super.toString
    }

    class DomainNot0EnumerationStep(override val currentId: Long,override val wordId: Int, var domain: Array[Int] //IntCursor cursor;
                                   ) extends EnumerationStep(currentId, wordId) {
      private[domain] val pos: Int = -1

      override def toString: String = "DomainNot0EnumerationStep{" + "cursor=" + domain + "} " + super.toString
    }

  }
}
