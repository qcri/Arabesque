package io.arabesque.Compression

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.koloboke.collect.IntCollection
import com.koloboke.collect.set.hash.{HashIntSet, HashIntSets}
import io.arabesque.computation.Computation
import io.arabesque.conf.Configuration
import io.arabesque.embedding.{EdgeInducedEmbedding, Embedding, VertexInducedEmbedding}
import io.arabesque.graph.{Edge, LabelledEdge, MainGraph}
import io.arabesque.odag.domain.StorageReader
import io.arabesque.pattern.{LabelledPatternEdge, Pattern, PatternEdge, PatternEdgeArrayList}
import io.arabesque.utils.collection.{IntArrayList, IntCollectionAddConsumer}

import scala.util.control.Breaks._
import scala.collection.JavaConversions._

/**
  * Created by ehussein on 6/27/17.
  */
class SimpleDomainStorageReadOnly extends SimpleDomainStorage {
  //private val LOG = Logger.getLogger(classOf[SimpleDomainStorageReadOnly])
  setLogLevel(Configuration.get[Configuration[Embedding]]().getLogLevel)
  setNumberOfDomains(numberOfDomains)
  /*
  @Override
  @throws(classOf[IOException])
  override def readFields (dataInput: DataInput): Unit = {
    this.clear()
    numEmbeddings = dataInput.readLong()
    setNumberOfDomains(dataInput.readInt())

    var i = 0
    while(i < numberOfDomains) {
      val domainSize = dataInput.readInt()
      var j = 0

      while(j < domainSize) {
        domainEntries(i).add(dataInput.readInt())
        j += 1
      }
      i += 1
    }

    countsDirty = true
  }
  */

  @Override
  @throws[RuntimeException]
  override def getReader(pattern: Pattern, computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    logInfo(s"Getting  a reader from SimpleDomainStorageReadOnly with maxBlockSize=$maxBlockSize, numBlocks=$numBlocks, numPartitions=$numPartitions")
    new Reader(pattern, computation, numPartitions, numBlocks, maxBlockSize)
  }

  @Override
  @throws[RuntimeException]
  override def getReader(patterns: Array[Pattern], computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    //new MultiPatternReader(patterns, computation, numPartitions, numBlocks, maxBlockSize)
    throw new RuntimeException("Shouldn't use multi-pattern with SimpleStorage")
  }

  class Reader
    extends StorageReader {
    final private var mainGraph: MainGraph = _
    final private var reusableEmbedding: Embedding = _
    final private var numberOfEnumerations: Long = 0L
    final private var blockSize: Long = 0L
    final private var partitionId: Int = 0
    final private var enumerationStack: util.Deque[EnumerationStep] = _
    final private var singletonExtensionSet: HashIntSet = _
    final private var pattern: Pattern = _
    final private var computation: Computation[Embedding] = _
    final private var numPartitions: Int = _

    private var targetEnumId: Long = 0L
    private var edgesConsumer: EdgesConsumer = _
    private var edgeIds: IntArrayList = _

    def this(pattern: Pattern, computation: Computation[Embedding], numPartitions: Int, numBlocks: Int, maxBlockSize: Int) = {
      this()
      this.pattern = pattern
      this.computation = computation
      this.numPartitions = numPartitions

      mainGraph = Configuration.get[Configuration[Embedding]]().getMainGraph[MainGraph]()
      reusableEmbedding = Configuration.get[Configuration[Embedding]]().createEmbedding()

      this.numberOfEnumerations = getNumberOfEnumerations

      this.blockSize = Math.min(Math.max(numberOfEnumerations / numBlocks, 1L), maxBlockSize)

      enumerationStack = new util.ArrayDeque[EnumerationStep]
      enumerationStack.add(new Domain0EnumerationStep(0, -1, -1))

      singletonExtensionSet = HashIntSets.newMutableSet(1)

      partitionId = computation.getPartitionId

      targetEnumId = -1

      edgeIds = new IntArrayList

      edgesConsumer = new EdgesConsumer(Configuration.get[Configuration[Embedding]]().isGraphEdgeLabelled)
      edgesConsumer.setCollection(edgeIds)

      logInfo(s"Inside ctor of SimpleDomainStorageReadOnly.Reader with \n{" +
        s"\nreusableEmbedding=${reusableEmbedding.toOutputString}," +
        s"\nblockSize=${this.blockSize}," +
        s"partitionId=$partitionId," +
        s"numberOfEnumerations=$numberOfEnumerations" +
        s"}")
    }

    override def hasNext: Boolean = moveNext

    override def next: Embedding = reusableEmbedding

    override def remove(): Unit = {
      throw new UnsupportedOperationException
    }

    private def tryAddWord(wordId: Int): Boolean = reusableEmbedding match {
      case reusableVertexEmbedding: VertexInducedEmbedding =>
        logInfo(s"Trying to add wordId=$wordId")
        //val reusableVertexEmbedding: VertexInducedEmbedding = reusableEmbedding.asInstanceOf[VertexInducedEmbedding]
        val vertices: IntArrayList = reusableVertexEmbedding.getVertices
        // now let's check if the word has already been added to the embedding
        if (vertices.contains(wordId))
          return false

        singletonExtensionSet.clear()
        singletonExtensionSet.add(wordId)

        computation.filter(reusableEmbedding, singletonExtensionSet)

        if (singletonExtensionSet.size == 0)
          return false

        // check if it is a canonical embedding with this word
        if (!computation.filter(reusableVertexEmbedding, wordId)) {
          logInfo(s"Adding wordId=$wordId into ${reusableVertexEmbedding.toOutputString} does not produce a canonical embedding")
          return false
        }

        logInfo(s"WordId = $wordId was added successfully to embedding ${reusableVertexEmbedding.toOutputString}")

        reusableVertexEmbedding.addWord(wordId)

        true

      case reusableEdgeEmbedding:EdgeInducedEmbedding =>
        //val reusableEdgeEmbedding: EdgeInducedEmbedding = reusableEmbedding.asInstanceOf[EdgeInducedEmbedding]
        logInfo(s"Trying to add wordId=$wordId")

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

        if (!computation.filter(reusableEdgeEmbedding, wordId)) {
          logInfo(s"Adding wordId=$wordId into ${reusableEdgeEmbedding.toOutputString} does not produce a canonical embedding")
          return false
        }

        reusableEdgeEmbedding.addWord(wordId)

        logInfo(s"WordId = $wordId was added successfully to embedding ${reusableEdgeEmbedding.toOutputString}")

        true

      case _ =>
        throw new RuntimeException("Incompatible embedding class: " + reusableEmbedding.getClass)
    }

    private def getEdgeIds(srcId: Int, dstId: Int, patternEdge: PatternEdge): IntCollection = {
      edgeIds.clear()
      edgesConsumer.setPatternEdge(patternEdge)
      mainGraph.forEachEdgeId(srcId, dstId, edgesConsumer)
      edgeIds
    }

    private def testCompleteEmbedding: Boolean = {
      logInfo(s"Test Complete Embedding where:")
      logInfo(s"reusableEmbedding=${reusableEmbedding.toOutputString}")
      logInfo(s"pattern=${pattern.toOutputString}")

      if (reusableEmbedding.getNumVertices != pattern.getNumberOfVertices) {
        logInfo("reusableEmbedding.getNumVertices != pattern.getNumberOfVertices")
        return false
      }

      if (reusableEmbedding.isInstanceOf[VertexInducedEmbedding]) {
        val reusableVertexEmbedding: VertexInducedEmbedding = reusableEmbedding.asInstanceOf[VertexInducedEmbedding]
        // Test if constructed embedding matches the pattern.
        // TODO: Perhaps we can do this incrementally in an efficient manner?
        val numEdgesPattern: Int = pattern.getNumberOfEdges
        val numEdgesEmbedding: Int = reusableVertexEmbedding.getNumEdges

        if (numEdgesEmbedding != numEdgesPattern)
          return false

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
      //computation.filter(reusableEmbedding) && computation.shouldExpand(reusableEmbedding)
      //*
      val filtered = computation.filter(reusableEmbedding)
      val shouldExpand = computation.shouldExpand(reusableEmbedding)
      logInfo(s"Computation.filter(embedding)=$filtered")
      logInfo(s"Computation.shouldExpand(embedding)=$shouldExpand")
      filtered && shouldExpand
      //*/
    }

    def getEnumerationWithStack(targetSize: Int): Boolean = {
      var currentId: Long = 0

      while (!enumerationStack.isEmpty && targetEnumId >= currentId) {
        val lastEnumerationStep: EnumerationStep = enumerationStack.pop
        val domainOfLastEnumerationStep: Int = enumerationStack.size
        val wordIdOfLastEnumerationStep: Int = lastEnumerationStep.wordId
        currentId = lastEnumerationStep.currentId

        if (wordIdOfLastEnumerationStep >= 0) {
          //currentId += domainEntries(domainOfLastEnumerationStep).get(wordIdOfLastEnumerationStep).getCounter
          currentId += domainCounters(domainOfLastEnumerationStep)
          reusableEmbedding.removeLastWord()
        }

        val domainWithPointers: Int = enumerationStack.size - 1

        // we are in the first domain: Domain0 -> Domain0EnumerationStep
        // Need to increment index of first domain
        if (domainWithPointers == -1) {
          val domain0EnumerationStep: Domain0EnumerationStep = lastEnumerationStep.asInstanceOf[Domain0EnumerationStep]

          var currentIndex: Int = domain0EnumerationStep.index

          val domain0: ConcurrentHashMap[Int, Boolean] = domainEntries.get(0)

          currentIndex += 1
          while (currentIndex < domain0OrderedKeys.length) {
            val wordId: Int = domain0OrderedKeys(currentIndex)
            //val newPossibilityForDomain0: DomainEntry = domain0.get(wordId)
            val domain0Counter: Long = domainCounters(0)
            //if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForDomain0.getCounter > targetEnumId)
            //  || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
            if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + domain0Counter > targetEnumId)
              || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
              var invalid: Boolean = false

              // If we couldn't add this word this means that the
              // current partial embedding and all extensions are invalid
              // so skip everything and return false since enumId was associated
              // with an invalid embedding.
              if (!tryAddWord(wordId)) {
                //targetEnumId = currentId + newPossibilityForDomain0.getCounter - 1
                targetEnumId = currentId + domain0Counter - 1
                invalid = true
                // Add word anyway. Embedding will be invalid with this word but it will be
                // popped on the next iteration of the while
                reusableEmbedding.addWord(wordId)
              }

              domain0EnumerationStep.index = currentIndex
              domain0EnumerationStep.currentId = currentId
              domain0EnumerationStep.wordId = wordId
              enumerationStack.push(domain0EnumerationStep)

              if (invalid)
                return false
              else {
                if (enumerationStack.size != targetSize) {
                  //val oneee: DomainEntryReadOnly = newPossibilityForDomain0.asInstanceOf[DomainEntryReadOnly]
                  //enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers))
                  // the next two lines replace the last two lines
                  // TODO we can optimize nextDomainPointers = wordID.neighbours intersection nextDomainPointers
                  val nextDomainPointers: Array[Int] = getWordIdsOfDomain(1)
                  enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, nextDomainPointers))
                }
                break
              }
            }
            //currentId += newPossibilityForDomain0.getCounter
            currentId += domainCounters(0)
            currentIndex += 1
          }
        }
        else { // we are now in one of the non-0 domains: Domain0 -> DomainNot0EnumerationStep
          val domainNot0EnumerationStep: DomainNot0EnumerationStep = lastEnumerationStep.asInstanceOf[DomainNot0EnumerationStep]
          //val possibilitiesLastDomain: util.Set[Int] = domainEntries(domainOfLastEnumerationStep)
          //val possibilitiesLastDomain: ConcurrentHashMap[Integer, Boolean] = domainEntries(domainOfLastEnumerationStep)
          val possibilitiesLastDomain: Array[Int] = getWordIdsOfDomain(domainOfLastEnumerationStep)
          val pointers: Array[Int] = domainNot0EnumerationStep.domain

          var i: Int = domainNot0EnumerationStep.pos + 1

          while (i < pointers.length) {
            val newWordId: Int = pointers(i)
            //val newPossibilityForLastDomain: DomainEntry = possibilitiesLastDomain.get(newWordId)
            val newPossibilityForLastDomain: Array[Int] = getWordIdsOfDomain(domainOfLastEnumerationStep + 1)
            val numOfNewPossibilities: Long = domainCounters(domainOfLastEnumerationStep)

            assert(newPossibilityForLastDomain != null)

            //if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForLastDomain.getCounter > targetEnumId)
            //  || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
            if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + numOfNewPossibilities > targetEnumId)
              || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
              var invalid: Boolean = false
              if (!tryAddWord(newWordId)) {
                //targetEnumId = currentId + newPossibilityForLastDomain.getCounter - 1
                targetEnumId = currentId + numOfNewPossibilities - 1
                invalid = true
                reusableEmbedding.addWord(newWordId)
              }
              lastEnumerationStep.currentId = currentId
              lastEnumerationStep.wordId = newWordId
              lastEnumerationStep.asInstanceOf[DomainNot0EnumerationStep].pos = i
              enumerationStack.push(lastEnumerationStep)
              if (invalid)
                return false
              else {
                if (enumerationStack.size != targetSize) {
                  //val oneee: DomainEntryReadOnly = newPossibilityForLastDomain.asInstanceOf[DomainEntryReadOnly]
                  //enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers))
                  enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, possibilitiesLastDomain))
                }
                break //todo: break is not supported
              }
            }
            //currentId += newPossibilityForLastDomain.getCounter
            currentId += numOfNewPossibilities

            i += 1
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
        if (targetEnumId == -1)
          return false
        if (getEnumerationWithStack(domainEntries.size))
          return true
      }
      false
    }

    def getNextEnumerationId(enumId: Long): Long = {
      logInfo(s"CurrentEnumId = $enumId")
      var enumCounter = enumId
      while (enumCounter < numberOfEnumerations - 1) {
        enumCounter += 1

        val currentBlockId: Long = enumCounter / blockSize

        if (isThisMyBlock(currentBlockId)) {
          logInfo(s"Yes this is my block($currentBlockId), and my NextEnumId = $enumCounter")
          return enumCounter
        }
        else // -1 because we'll increment it at the beginning of the next iteration
          enumCounter = (currentBlockId + blocksToSkip(currentBlockId)) * blockSize - 1
      }
      if (enumCounter >= numberOfEnumerations - 1)
        enumCounter = -1
      logInfo(s"NextEnumId = $enumCounter")
      enumCounter
    }

    def blocksToSkip(blockId: Long): Int = {
      val owningPartition: Int = (blockId % numPartitions).toInt
      var myPartition: Int = partitionId
      if (myPartition < owningPartition)
        myPartition += numPartitions
      myPartition - owningPartition
    }

    def isThisMyBlock(blockId: Long): Boolean = {
      //(blockId % numPartitions) == partitionId
      logInfo(s"I am partition # $partitionId and this blockId = $blockId")
      val isItMyBlock = (blockId % numPartitions) == partitionId
      logInfo(s"And isItMyBlock = $isItMyBlock")
      isItMyBlock
    }

    override def close(): Unit = {
      // Do nothing by default
    }

    abstract class EnumerationStep {
      var currentId: Long = _
      var wordId: Int = _

      def this(currentId: Long, wordId: Int) = {
        this
        this.currentId = currentId
        this.wordId = wordId
      }
      override def toString: String = {
        s"EnumerationStep{currentId=$currentId, wordId=$wordId}"
      }
    }

    class Domain0EnumerationStep extends EnumerationStep {
      var index: Int = _

      def this(currentId: Long, wordId: Int, index: Int) = {
        this()
        this.currentId = currentId
        this.wordId = wordId
        this.index = index
      }
      override def toString: String = "Domain0EnumerationStep{" + "index=" + index + "} " + super.toString

    }

    class DomainNot0EnumerationStep extends EnumerationStep {

      var pos: Int = -1
      var domain: Array[Int] = _

      def this(currentId: Long, wordId: Int, domain: Array[Int]) = {
        this()
        this.currentId = currentId
        this.wordId = wordId
        this.domain = domain
      }

      override def toString: String = "DomainNot0EnumerationStep{" + "cursor=" + domain + "} " + super.toString
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
  }
}
