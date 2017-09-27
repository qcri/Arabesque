package io.arabesque.compression

import java.util
import java.io._

import com.koloboke.collect.IntCollection
import com.koloboke.collect.set.hash.{HashIntSet, HashIntSets}
import io.arabesque.computation.Computation
import io.arabesque.conf.Configuration
import io.arabesque.embedding.{EdgeInducedEmbedding, Embedding, VertexInducedEmbedding}
import io.arabesque.graph.{Edge, LabelledEdge, MainGraph}
import io.arabesque.odag.domain.StorageReader
import io.arabesque.pattern.{LabelledPatternEdge, Pattern, PatternEdge, PatternEdgeArrayList}
import io.arabesque.report.StorageReport
import io.arabesque.utils.collection.{IntArrayList, IntCollectionAddConsumer}

import scala.util.control.Breaks._
//import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ehussein on 6/27/17.
  */
class SimpleDomainStorageReadOnly extends SimpleDomainStorage {
  //private val LOG = Logger.getLogger(classOf[SimpleDomainStorageReadOnly])
  //setLogLevel(Configuration.get[Configuration[Embedding]]().getLogLevel)
  setNumberOfDomains(numberOfDomains)
  //*
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
        domainEntries.get(i).put(dataInput.readInt(), dataInput.readBoolean())
        j += 1
      }
      i += 1
    }

    countsDirty = true

    isStorageInitialized = false
    initStorage()
    //initReport()
  }
  //*/

  @Override
  @throws[RuntimeException]
  override def getReader(pattern: Pattern, computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    //logInfo(s"Getting  a reader from SimpleDomainStorageReadOnly with maxBlockSize=$maxBlockSize, numBlocks=$numBlocks, numPartitions=$numPartitions")
    new Reader(pattern, computation, numPartitions, numBlocks, maxBlockSize)
  }

  @Override
  @throws[RuntimeException]
  override def getReader(patterns: Array[Pattern], computation: Computation[Embedding],
                numPartitions: Int, numBlocks: Int, maxBlockSize: Int): StorageReader = {
    //new MultiPatternReader(patterns, computation, numPartitions, numBlocks, maxBlockSize)
    throw new RuntimeException("Multi-pattern with SimpleStorage is not available")
  }

  //*
  // Efficient Storage
  private var storage: Array[Array[Int]] = _
  protected var isStorageInitialized: Boolean = false

  def initStorage(): Unit = {
    if(isStorageInitialized)
      return

    storage = new Array[Array[Int]](numberOfDomains)

    var i = 0
    while(i < numberOfDomains) {
      val words: Array[Int] = Array.ofDim(domainEntries.get(i).size)
      val keysSet = domainEntries.get(i).keys

      var j = 0
      while(keysSet.hasMoreElements) {
        words(j) = keysSet.nextElement()
        j += 1
      }

      storage(i) = words
      i += 1
    }
    isStorageInitialized = true
  }

  //*/

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
    final private var superStep: Int = _

    private var targetEnumId: Long = 0L
    private var edgesConsumer: EdgesConsumer = _
    private var edgeIds: IntArrayList = _

    // Debugging flags
    val DEBUG_TestCompleteEmbedding:Boolean = false
    val DEBUG_TryAddWord:Boolean = false
    val DEBUG_GetNextEnumerationID:Boolean = false
    val DEBUG_GetEnumerationWithStack:Boolean = false
    val DEBUG_CTOR: Boolean = false
    val DEBUG_MOVE_NEXT = false
    val TARGET_SUPER_STEP: Int = 3
    var isItTargetSuperStep: Boolean = false

    protected var report: StorageReport = new StorageReport
    protected var numCompleteEnumerationsVisited:Long = 0
    // how many invalid embeddings this storage/partition generated
    protected var numSpuriousEmbeddings: Long = 0L

    def this(pattern: Pattern, computation: Computation[Embedding], numPartitions: Int, numBlocks: Int, maxBlockSize: Int) = {
      this()
      this.pattern = pattern
      this.computation = computation
      this.numPartitions = numPartitions

      mainGraph = Configuration.get[Configuration[Embedding]]().getMainGraph[MainGraph]()
      reusableEmbedding = Configuration.get[Configuration[Embedding]]().createEmbedding()

      this.numberOfEnumerations = getNumberOfEnumerations

      this.blockSize = Math.min(Math.max(numberOfEnumerations / numBlocks, 1L), maxBlockSize)

      this.superStep = computation.getStep

      enumerationStack = new util.ArrayDeque[EnumerationStep]
      enumerationStack.add(new Domain0EnumerationStep(0, -1, -1))

      singletonExtensionSet = HashIntSets.newMutableSet(1)

      partitionId = computation.getPartitionId

      targetEnumId = -1

      edgeIds = new IntArrayList

      edgesConsumer = new EdgesConsumer(Configuration.get[Configuration[Embedding]]().isGraphEdgeLabelled)
      edgesConsumer.setCollection(edgeIds)

      isItTargetSuperStep = superStep == TARGET_SUPER_STEP

      if(DEBUG_CTOR && isItTargetSuperStep){
        printDebugInfo("ctor", "")
      }

      report.initReport(numberOfDomains)
      //initStorage()
    }

    //*
    protected def getWordIdsOfDomain(domainId: Int) : Array[Int] = {
      if(domainId >= numberOfDomains || domainId < 0)
        throw new ArrayIndexOutOfBoundsException(s"Should not access domain $domainId while numOfDomain=$numberOfDomains")
      storage(domainId)
    }
    //*/

    def finalizeReport(): Unit = {
      report.numEnumerations = getNumberOfEnumerations
      report.numCompleteEnumerationsVisited = numCompleteEnumerationsVisited
      report.numSpuriousEmbeddings = numSpuriousEmbeddings
      report.numActualEmbeddings = numEmbeddings

      var i = 0
      while(i < numberOfDomains) {
        report.domainSize(i) = domainEntries.get(i).size()
        i += 1
      }
    }
    //*/

    def printDebugInfo(callerName: String, message: String) = {
      println(s"\nInside $callerName (partitionId=$partitionId) " +
        s"in SuperStep($superStep) with {\n" +
        s"\nnumberOfEnumerations=$numberOfEnumerations" +
        s"\nblockSize=${this.blockSize}" +
        s"\n" + message +
        s"\n}")
    }

    // check if the reusable embedding is the target embedding
    private def isItTargetEmbedding: Boolean = {

      if(reusableEmbedding.getNumWords == 0)
        return false

      val targetEmbedding: Array[Int] = Array(2641, 2686, 2893, 3254)
      val words = reusableEmbedding.getWords

      var i = 0

      while(i < words.size()) {
        if(words.getUnchecked(i) != targetEmbedding(i))
          return false
        i += 1
      }

      return true
    }

    override def hasNext: Boolean = moveNext

    override def next: Embedding = reusableEmbedding

    override def remove(): Unit = {
      throw new UnsupportedOperationException
    }

    private def tryAddWord(wordId: Int): Boolean = reusableEmbedding match {
      case reusableVertexEmbedding: VertexInducedEmbedding =>
        val vertices: IntArrayList = reusableVertexEmbedding.getVertices

        val isTargetEmbedding:Boolean = false//isItTargetEmbedding
        val isTargetSuperStep:Boolean = (superStep == TARGET_SUPER_STEP)
        val callerName = "tryAddWord"

        if(DEBUG_TryAddWord && isTargetSuperStep && isTargetEmbedding) {
          val message = s"Trying to add Word $wordId to the Target Embedding = ${reusableEmbedding.toOutputString}"
          printDebugInfo(callerName, message)
        }

        // now let's check if the word has already been added to the embedding
        if (vertices.contains(wordId))
          return false

        /*
        if(DEBUG_TryAddWord && isTargetSuperStep && isTargetEmbedding) {
          val message = s"I am in TryAddWord and word $wordId is not part of the embedding"
          printDebugInfo(callerName, message)
        }
        */

        singletonExtensionSet.clear()
        singletonExtensionSet.add(wordId)

        computation.filter(reusableEmbedding, singletonExtensionSet)

        if (singletonExtensionSet.size == 0)
          return false

        // check if it is a canonical embedding with this word
        if (!computation.filter(reusableVertexEmbedding, wordId)) {
          //*
          if(DEBUG_TryAddWord && isTargetSuperStep && isTargetEmbedding) {
            val message = s"This word $wordId + Embedding are not canonical embedding!"
            printDebugInfo(callerName, message)
          }
          //*/
          return false
        }

        if(DEBUG_TryAddWord && isTargetSuperStep && isTargetEmbedding) {
          val message = s"Adding Word $wordId successfully to the Target Embedding = {${reusableEmbedding.toOutputString}}"
          printDebugInfo(callerName, message)
        }

        reusableVertexEmbedding.addWord(wordId)

        if(DEBUG_TryAddWord && isTargetSuperStep && isTargetEmbedding) {
          val message = s"New Target Embedding = {${reusableEmbedding.toOutputString}} " +
            s"after adding Word $wordId successfully"
          printDebugInfo(callerName, message)
        }

        return true

      case reusableEdgeEmbedding:EdgeInducedEmbedding =>
        //val reusableEdgeEmbedding: EdgeInducedEmbedding = reusableEmbedding.asInstanceOf[EdgeInducedEmbedding]
        //logInfo(s"Trying to add wordId=$wordId")

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
          //logInfo(s"Adding wordId=$wordId into ${reusableEdgeEmbedding.toOutputString} does not produce a canonical embedding")
          return false
        }

        reusableEdgeEmbedding.addWord(wordId)

        //logInfo(s"WordId = $wordId was added successfully to embedding ${reusableEdgeEmbedding.toOutputString}")

        return true

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

      if (reusableEmbedding.getNumVertices != pattern.getNumberOfVertices) {
        //logInfo("reusableEmbedding.getNumVertices != pattern.getNumberOfVertices")
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

      val filtered = computation.filter(reusableEmbedding)
      val shouldExpand = computation.shouldExpand(reusableEmbedding)

      if(DEBUG_TestCompleteEmbedding && isItTargetEmbedding) {
        println("I am in TestCompleteEmbedding!")
        println("This is target embedding: " + reusableEmbedding.toOutputString)
        println(s"Filtered = $filtered")
        println(s"ShouldExpand = $shouldExpand")
      }

      /*
      // if it is spurious
      if(!filtered || !shouldExpand)
        numSpuriousEmbeddings += 1
      */

      return filtered && shouldExpand
    }

    def getEnumerationWithStack(targetSize: Int): Boolean = {
      var currentId: Long = 0
      val isTargetEmbedding:Boolean = false//isItTargetEmbedding
      val callerName = "getEnumerationWithStack"

      breakable {
        while (!enumerationStack.isEmpty && targetEnumId >= currentId) {
          val lastEnumerationStep: EnumerationStep = enumerationStack.pop
          val domainOfLastEnumerationStep: Int = enumerationStack.size
          val wordIdOfLastEnumerationStep: Int = lastEnumerationStep.wordId
          currentId = lastEnumerationStep.currentId

          if (wordIdOfLastEnumerationStep >= 0) {
            currentId += domainCounters(domainOfLastEnumerationStep)
            reusableEmbedding.removeLastWord()
          }

          val domainWithPointers: Int = enumerationStack.size - 1

          if(DEBUG_GetEnumerationWithStack && isItTargetEmbedding && isItTargetSuperStep) {
            val message = s"The target embedding = { ${reusableEmbedding.toOutputString} }" +
              s"\ncurrentEnumId = $currentId" +
              s"\n"
            printDebugInfo(callerName, message)
          }

          // we are in the first domain: Domain0 -> Domain0EnumerationStep
          // Need to increment index of first domain
          if (domainWithPointers == -1) {

            val domain0EnumerationStep: Domain0EnumerationStep = lastEnumerationStep.asInstanceOf[Domain0EnumerationStep]

            var currentIndex: Int = domain0EnumerationStep.index

            currentIndex += 1
            breakable {
              while (currentIndex < domain0OrderedKeys.length) {
                val wordId: Int = domain0OrderedKeys(currentIndex)
                val domain0Counter: Long = domainCounters(0)

                if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + domain0Counter > targetEnumId)
                  || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
                  var invalid: Boolean = false

                  // If we couldn't add this word this means that the
                  // current partial embedding and all extensions are invalid
                  // so skip everything and return false since enumId was associated
                  // with an invalid embedding.
                  if (!tryAddWord(wordId)) {
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

                  if (invalid) {
                    numSpuriousEmbeddings += 1
                    report.pruned(domainOfLastEnumerationStep) += 1
                    return false
                  }
                  else {
                    report.explored(domainOfLastEnumerationStep) += 1
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
                currentId += domainCounters(0)
                currentIndex += 1
              }
            }
          }
          else {
            if(DEBUG_GetEnumerationWithStack && reusableEmbedding.getVertices().size() >= 1 && isItTargetEmbedding) {
              println("I am in getEnumerationWithStack#DomainNot0EnumerationStep!")
              println("This is target embedding: " + reusableEmbedding.toOutputString)
              println(s"while PartitionId = $partitionId")
            }
            // we are now in one of the non-0 domains: Domain0 -> DomainNot0EnumerationStep
            val domainNot0EnumerationStep: DomainNot0EnumerationStep = lastEnumerationStep.asInstanceOf[DomainNot0EnumerationStep]

            val possibilitiesLastDomain: Array[Int] = {
              if(domainOfLastEnumerationStep + 1 ==  numberOfDomains)
                new Array[Int](0)
              else
                getWordIdsOfDomain(domainOfLastEnumerationStep + 1)
            }

            val pointers: Array[Int] = domainNot0EnumerationStep.domain

            var i: Int = domainNot0EnumerationStep.pos + 1

            breakable {
              while (i < pointers.length) {
                val newWordId: Int = pointers(i)
                val numOfNewPossibilities: Long = domainCounters(domainOfLastEnumerationStep)

                if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + numOfNewPossibilities > targetEnumId)
                  || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
                  var invalid: Boolean = false
                  if (!tryAddWord(newWordId)) {
                    targetEnumId = currentId + numOfNewPossibilities - 1
                    invalid = true
                    reusableEmbedding.addWord(newWordId)
                  }
                  lastEnumerationStep.currentId = currentId
                  lastEnumerationStep.wordId = newWordId
                  lastEnumerationStep.asInstanceOf[DomainNot0EnumerationStep].pos = i
                  enumerationStack.push(lastEnumerationStep)
                  if (invalid) {
                    report.pruned(domainOfLastEnumerationStep) += 1
                    numSpuriousEmbeddings += 1
                    return false
                  }
                  else {
                    report.explored(domainOfLastEnumerationStep) += 1
                    if (enumerationStack.size != targetSize) {
                      //val oneee: DomainEntryReadOnly = newPossibilityForLastDomain.asInstanceOf[DomainEntryReadOnly]
                      //enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers))
                      enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, possibilitiesLastDomain))
                    }
                    break
                  }
                }
                currentId += numOfNewPossibilities

                i += 1
              }
            }
          }
          // If enumeration stack is of the desired size
          if (enumerationStack.size == targetSize) { // And last element actually represents a valid element
            if (enumerationStack.peek.wordId >= 0) { // Get out of the loop
              break
            }
          }
        }
      }

      numCompleteEnumerationsVisited += 1
      val isCompleteEmbeddingValid = testCompleteEmbedding
      val isEmbeddingOfTargetSize = reusableEmbedding.getNumWords == targetSize

      if(!(isCompleteEmbeddingValid && isEmbeddingOfTargetSize))
        numSpuriousEmbeddings += 1

      isEmbeddingOfTargetSize && isCompleteEmbeddingValid
    }

    def toStringResume: String = {
      val sb: StringBuilder = new StringBuilder
      sb.append("EmbeddingsZip Reader:\n")
      sb.append("Enumerations: " + targetEnumId + " " + numberOfEnumerations + "\n")
      sb.toString
    }

    def moveNext: Boolean = {
      var isTargetEmbedding:Boolean = false
      var previousTargetEnumID:Long = -1
      val callerName = "moveNext"

      while (true) {
        previousTargetEnumID = targetEnumId
        targetEnumId = getNextEnumerationId(targetEnumId)


        if(DEBUG_MOVE_NEXT && isTargetEmbedding && isItTargetSuperStep) {
          val message = s"The target embedding = { ${reusableEmbedding.toOutputString} }" +
            s"\npreviousTargetEnumID=$previousTargetEnumID" +
            s"\ntargetEnumId = $targetEnumId" +
            s"\n"
          printDebugInfo(callerName, message)
        }


        if (targetEnumId == -1)
          return false

        val GetEnumStack = getEnumerationWithStack(domainEntries.size)

        isTargetEmbedding = false//isItTargetEmbedding

        if (GetEnumStack) {
          if(DEBUG_MOVE_NEXT && isTargetEmbedding && isItTargetSuperStep) {
            val message = s"Yes, we got enumeration stack and The target embedding = { ${reusableEmbedding.toOutputString} }" +
              s"\ntargetEnumId = $targetEnumId" +
              s"\ngetEnumBlockID=${getEnumBlockID(targetEnumId)}" +
              s"\nisThisMyBlock=${isThisMyBlock(getEnumBlockID(targetEnumId))}"
            printDebugInfo(callerName, message)
            printAllEnumerations(s"/home/ehussein/Downloads/ArabesqueTesting/compresssion/storage_enums/enums${numberOfEnumerations}")
          }
          return true
        }
        else {
          if(DEBUG_MOVE_NEXT && isTargetEmbedding && isItTargetSuperStep) {
            val message = s"No, we could not get enumeration stack and The target embedding = { ${reusableEmbedding.toOutputString} }" +
              s"\ntargetEnumId = $targetEnumId" +
              s"\n"
            printDebugInfo(callerName, message)
          }
        }
      }
      false

      /*
      while (true) {
        targetEnumId = getNextEnumerationId(targetEnumId)

        if (targetEnumId == -1)
          return false

        if (getEnumerationWithStack(domainEntries.size))
          return true
      }
      false
      */
    }

    def getEnumBlockID(enumId: Long) = enumId / blockSize

    def getNextEnumerationId(enumId: Long): Long = {
      val isTargetEmbedding:Boolean = false//isItTargetEmbedding
      val callerName = "getNextEnumerationId"

      if(DEBUG_GetNextEnumerationID && isItTargetSuperStep && isTargetEmbedding) {
        val message = s"The target embedding = { ${reusableEmbedding.toOutputString} }" +
          s"\ncurrentEnumId = $enumId"
        printDebugInfo(callerName, message)
      }

      var enumCounter: Long = enumId
      while (enumCounter < numberOfEnumerations - 1) {
        enumCounter += 1

        val currentBlockId: Long = enumCounter / blockSize

        if(DEBUG_GetNextEnumerationID && isItTargetSuperStep && isTargetEmbedding) {
          val message = s"\ncurrentBlockId = $currentBlockId"
          printDebugInfo(callerName, message)
        }

        if (isThisMyBlock(currentBlockId)) {
          if(DEBUG_GetNextEnumerationID && isItTargetSuperStep && isTargetEmbedding) {
            val message = s"\nYes this is my block($currentBlockId), and my NextEnumId = $enumCounter"
            printDebugInfo(callerName, message)
          }
          return enumCounter
        }
        else // -1 because we'll increment it at the beginning of the next iteration
          enumCounter = (currentBlockId + blocksToSkip(currentBlockId)) * blockSize - 1
      }
      if (enumCounter >= numberOfEnumerations - 1)
        enumCounter = -1
      //logInfo(s"NextEnumId = $enumCounter")

      if(DEBUG_GetNextEnumerationID && isItTargetSuperStep && isTargetEmbedding) {
        val message = s"\nnextEnumId = $enumCounter"
        printDebugInfo(callerName, message)}

      return enumCounter
    }

    def blocksToSkip(blockId: Long): Int = {
      val owningPartition: Int = (blockId % numPartitions).toInt
      var myPartition: Int = partitionId

      if (myPartition < owningPartition)
        myPartition += numPartitions

      return myPartition - owningPartition
    }

    def isThisMyBlock(blockId: Long): Boolean = {
      (blockId % numPartitions) == partitionId
    }

    override def close(): Unit = {
      // Do nothing by default
    }

    def getStorageReport(): StorageReport = {
      finalizeReport()
      report
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
