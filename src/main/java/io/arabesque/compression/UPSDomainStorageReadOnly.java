package io.arabesque.compression;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.LabelledEdge;
import io.arabesque.graph.MainGraph;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.pattern.LabelledPatternEdge;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.PatternEdge;
import io.arabesque.pattern.PatternEdgeArrayList;
import io.arabesque.report.StorageReport;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;

//import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class UPSDomainStorageReadOnly extends UPSDomainStorage {
    //private static final Logger LOG = Logger.getLogger(UPSDomainStorageReadOnly.class);

    @Override
    public void readFields(DataInput dataInput) throws IOException { 
        this.clear();

        numEmbeddings = dataInput.readLong();
        setNumberOfDomains(dataInput.readInt());

        for (int i = 0; i < numberOfDomains; ++i) {
            int domainSize = dataInput.readInt();

            for (int j = 0; j < domainSize; ++j) {
                domainEntries.get(i).add(dataInput.readInt());
            }
        }
        countsDirty = true;

        isStorageInitialized = false;
        initStorage();
    }

    @Override
    public StorageReader getReader(Pattern pattern,
            Computation<Embedding> computation,
            int numPartitions, int numBlocks, int maxBlockSize) {
        return new Reader(pattern, computation, numPartitions, numBlocks, maxBlockSize);
    }

    @Override
    public StorageReader getReader(Pattern[] patterns,
            Computation<Embedding> computation,
            int numPartitions, int numBlocks, int maxBlockSize) {
        throw new RuntimeException("Multi-pattern with SimpleStorage is not available");
    }

    // Reading efficient storage
    private int[][] storage = null;
    protected boolean isStorageInitialized = false;

    public long getReadingStorageSize() {
        long size = 0;

        for (int i = 0 ; i < storage.length ; ++i) {
            size += storage[i].length;
        }

        return size * 4;
    }

    private void initStorage() {
        if(isStorageInitialized)
            return;

        storage = new int[numberOfDomains][];

        for(int i = 0 ; i < numberOfDomains ; ++ i) {
            storage[i] = new int[domainEntries.get(i).size()];
            int[] keysSet = domainEntries.get(i).toIntArray();

            for(int j = 0 ; j < keysSet.length ; ++j)
                storage[i][j] = keysSet[j];
        }

        isStorageInitialized = true;
    }

    public class Reader implements StorageReader {

        private final MainGraph mainGraph;
        private final Embedding reusableEmbedding;
        private final long numberOfEnumerations;

        private final long blockSize;
        private final int partitionId;

        private final Deque<EnumerationStep> enumerationStack;
        private final HashIntSet singletonExtensionSet;
        private final Pattern pattern;
        private final Computation<Embedding> computation;
        private final int numPartitions;
        private final int superStep;

        private long targetEnumId;

        private EdgesConsumer edgesConsumer;
        private IntArrayList edgeIds;

        // new enumeration
        //private int[] domainPointer;
        private int[] domain0Keys;
        private boolean hasMoreEnumerations = true;

        private final boolean debugCtor = false;
        // Debugging flags
        private final boolean DEBUG_TestCompleteEmbedding = false;
        private final boolean DEBUG_TryAddWord = false;
        private final boolean DEBUG_GetNextEnumerationID = false;
        private final boolean DEBUG_GetEnumerationWithStack = false;
        private final boolean DEBUG_CTOR = false;
        private final boolean DEBUG_MOVE_NEXT = false;
        private final int TARGET_SUPER_STEP = 3;
        private final boolean isItTargetSuperStep;

        protected StorageReport report = new StorageReport();
        protected long numCompleteEnumerationsVisited = 0;
        // how many invalid embeddings this storage/partition generated
        protected long numSpuriousEmbeddings = 0L;

        // Logging
        private String logName = this.getClass().getSimpleName();
        private Logger logger = LoggerFactory.getLogger(logName);

        public Reader(Pattern pattern, Computation<Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
            this.pattern = pattern;
            this.computation = computation;
            this.numPartitions = numPartitions;

            mainGraph = Configuration.get().getMainGraph();
            reusableEmbedding = Configuration.get().createEmbedding();

            this.numberOfEnumerations = getNumberOfEnumerations();

            this.blockSize = Math.min(Math.max(numberOfEnumerations / numBlocks, 1L), maxBlockSize);

            this.superStep = computation.getStep();

            enumerationStack = new ArrayDeque<>();
            enumerationStack.add(new Domain0EnumerationStep(-1, -1));

            singletonExtensionSet = HashIntSets.newMutableSet(1);

            partitionId = computation.getPartitionId();

            targetEnumId = -1;

            edgeIds = new IntArrayList();

            edgesConsumer = new EdgesConsumer(Configuration.get().isGraphEdgeLabelled());
            edgesConsumer.setCollection(edgeIds);

            isItTargetSuperStep = superStep == TARGET_SUPER_STEP;

            LogManager.getLogger(logName).setLevel(Level.toLevel(Configuration.get().getLogLevel()));

            if(DEBUG_CTOR && isItTargetSuperStep){
                printDebugInfo("ctor", "");
            }

            report.initReport(numberOfDomains);

            // new enumeration
            //initDomain0Keys();
            initDomain0KeysRoundRobin();
        }

        private void printDiffEdges() {
            int numEdges = mainGraph.getNumberEdges();

            for(int i = 0; i < numEdges ; ++i) {
                Edge e = mainGraph.getEdge(i);
                if(e.hasVertex(3089) && e.hasVertex(3101))
                    System.out.println(e.toString());
                if(e.hasVertex(3101) && e.hasVertex(3213))
                    System.out.println(e.toString());
            }
        }

        private void checkEdge(int edgeId) {
            Edge e = mainGraph.getEdge(edgeId);

            if(Arrays.binarySearch(domain0Keys, edgeId) >= 0) {
                System.out.println("Edge " + e + " found in partition " + partitionId);
            }
/*            else
                System.out.println("Could not find " + e);*/
        }

        private void initDomain0Keys() {
            int mod = storage[0].length % numPartitions;
            int share = storage[0].length / numPartitions;
            int start = 0;

            // if we are in the first mod partitions then give it an extra work load
            if (mod != 0 && partitionId < mod) {
                share += 1;
                start = share * partitionId;
            }
            else
                start = (share * partitionId) + mod;

            // if mod = 0 then we can balance the share equally
            domain0Keys = new int[share];

            for(int i = 0 ; i < domain0Keys.length ; ++i, ++start)
                domain0Keys[i] = domain0OrderedKeys[start];
        }

        private void initDomain0KeysRoundRobin() {
            int mod = storage[0].length % numPartitions;
            int share = storage[0].length / numPartitions;
            int start = 0;

            // if we are in the first mod partitions then give it an extra work load
            if (mod != 0 && partitionId < mod) {
                share += 1;
                start = share * partitionId;
            }
            else
                start = (share * partitionId) + mod;

            start = partitionId;

            // if mod = 0 then we can balance the share equally
            domain0Keys = new int[share];

            for(int i = 0 ; i < share ; ++i, start += numPartitions)
                domain0Keys[i] = domain0OrderedKeys[start];
        }

        private void printDomain0Keys() {
            System.out.println("@Superstep=" + superStep
                    + ", Partition (" + partitionId + ")" +
                    " will process " + domain0Keys.length +
                    " keys out of " + domain0OrderedKeys.length + " keys");
        }

        /*
        public void moveDomainPointer(int domainId) {

            if(domainId == 0) {
                // if domain0 pointer reached the end of the domain then it is the end
                if(domainPointer[0] == domain0Keys.length - 1) {
                    hasMoreEnumerations = false;
                    // start over
                    domainPointer[0] += 1; // now we are done
                }
                else
                    domainPointer[0] += 1;
                return;
            }

            // if pointer reached the end of the domain
            if(domainPointer[domainId] == storage[domainId].length - 1) {
                // start over
                domainPointer[domainId] = 0;
                // and update the pointer of the previous domain
                moveDomainPointer(domainId - 1);
            }
            else
                domainPointer[domainId] += 1;
        }
        */

        protected int[] getWordIdsOfDomain(int domainId) {
            if(domainId >= numberOfDomains || domainId < 0)
                throw new ArrayIndexOutOfBoundsException("Should not access domain " + domainId + " while numOfDomain=" + numberOfDomains);
            if(domainId == 0)
                return domain0Keys;
            else
                return storage[domainId];
        }

        protected void printDebugInfo(String callerName, String message) {
            String msg = "\nInside " + callerName +
                    "(partitionId=" + partitionId +
                    "in SuperStep " + superStep + " with {\n" +
                    "\nnumberOfEnumerations=" + numberOfEnumerations +
                    "\nblockSize=" + this.blockSize +
                    "\n" + message + "\n}";
            logger.info(msg);
        }

        protected void finalizeReport() {
            report.numEnumerations = numberOfEnumerations;
            report.numCompleteEnumerationsVisited = numCompleteEnumerationsVisited;
            report.numSpuriousEmbeddings = numSpuriousEmbeddings;
            report.numActualEmbeddings = numEmbeddings;

            for(int i = 0; i < numberOfDomains; ++i) {
                report.domainSize[i] = domainEntries.get(i).size();
            }
        }

        public StorageReport getStorageReport() {
            finalizeReport();
            return report;
        }

        @Override
        public boolean hasNext() {
            return moveNext();
        }

        @Override
        public Embedding next() {
            return reusableEmbedding;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean tryAddWord(int wordId) {
            if (reusableEmbedding instanceof VertexInducedEmbedding) {
                VertexInducedEmbedding reusableVertexEmbedding = (VertexInducedEmbedding) reusableEmbedding;

/*                int lastWordAdded = reusableVertexEmbedding.getWords().getLast();
                if(!mainGraph.isNeighborVertex(lastWordAdded,wordId))
                    return false;*/

                // now let's check if the word has already been added to the embedding (Trying to add existing vertex)
                IntArrayList vertices = reusableVertexEmbedding.getVertices();
                if(vertices.contains(wordId))
                    return false;

                singletonExtensionSet.clear();
                singletonExtensionSet.add(wordId);

                computation.filter(reusableEmbedding, singletonExtensionSet);

                if (singletonExtensionSet.size() == 0)
                    return false;

                // check if it is a canonical embedding with this word
                if (!computation.filter(reusableVertexEmbedding, wordId))
                    return false;

                reusableVertexEmbedding.addWord(wordId);
            } else if (reusableEmbedding instanceof EdgeInducedEmbedding) {
                EdgeInducedEmbedding reusableEdgeEmbedding = (EdgeInducedEmbedding) reusableEmbedding;

/*                int lastWordAdded = reusableEdgeEmbedding.getWords().getLast();
                if(!mainGraph.areEdgesNeighbors(lastWordAdded,wordId))
                    return false;*/

                singletonExtensionSet.clear();
                singletonExtensionSet.add(wordId);

                computation.filter(reusableEmbedding, singletonExtensionSet);

                PatternEdgeArrayList edges = pattern.getEdges();

                PatternEdge equivalentPatternEdge = edges.get(reusableEdgeEmbedding.getNumWords());

                int equivalentPatternEdgeSrcIndex = equivalentPatternEdge.getSrcPos();
                int equivalentPatternEdgeDestIndex = equivalentPatternEdge.getDestPos();

                reusableEdgeEmbedding.addWord(wordId);
                IntArrayList embeddingVertices = reusableEdgeEmbedding.getVertices();
                int numEmbeddingVertices = reusableEdgeEmbedding.getNumVertices();
                reusableEdgeEmbedding.removeLastWord();

                // If pattern has more vertices than embedding with this word, quit,
                // expansion not valid
                if (equivalentPatternEdgeSrcIndex >= numEmbeddingVertices ||
                        equivalentPatternEdgeDestIndex >= numEmbeddingVertices) {
                    return false;
                }

                // Otherwise, if same number of vertices, check if the edge connecting the
                // vertices mapped from the pattern is the same that we are trying to add.
                // If not, quit, expansion not valid.
                IntCollection edgeIds = getEdgeIds(
                        embeddingVertices.getUnchecked(equivalentPatternEdgeSrcIndex),
                        embeddingVertices.getUnchecked(equivalentPatternEdgeDestIndex),
                        equivalentPatternEdge);

                // NOTE: IntSet would theoretically allow faster contains but, in practice,
                // we assume not a lot of edges between 2 vertices exist with the same label
                // so array should be quicker.
                if (!edgeIds.contains(wordId))
                    return false;

                if (!computation.filter(reusableEdgeEmbedding, wordId))
                    return false;

                reusableEdgeEmbedding.addWord(wordId);
            } else {
                throw new RuntimeException("Incompatible embedding class: " + reusableEmbedding.getClass());
            }

            return true;
        }

        private IntCollection getEdgeIds(int srcId, int dstId, PatternEdge patternEdge) {
            edgeIds.clear();
            edgesConsumer.setPatternEdge(patternEdge);
            mainGraph.forEachEdgeId(srcId, dstId, edgesConsumer);
            return edgeIds;
        }

        private boolean testCompleteEmbedding() {
            if (reusableEmbedding.getNumVertices() != pattern.getNumberOfVertices())
                return false;

            if (reusableEmbedding instanceof VertexInducedEmbedding) {
                VertexInducedEmbedding reusableVertexEmbedding = (VertexInducedEmbedding) reusableEmbedding;

                // Test if constructed embedding matches the pattern.
                // TODO: Perhaps we can do this incrementally in an efficient manner?
                int numEdgesPattern = pattern.getNumberOfEdges();
                int numEdgesEmbedding = reusableVertexEmbedding.getNumEdges();

                if (numEdgesEmbedding != numEdgesPattern)
                    return false;

                PatternEdgeArrayList edgesPattern = pattern.getEdges();
                IntArrayList edgesEmbedding = reusableVertexEmbedding.getEdges();
                IntArrayList verticesEmbedding = reusableVertexEmbedding.getVertices();

                for (int i = 0; i < numEdgesPattern; ++i) {
                    PatternEdge edgePattern = edgesPattern.get(i);
                    Edge edgeEmbedding = mainGraph.getEdge(edgesEmbedding.getUnchecked(i));

                    if (!edgeEmbedding.hasVertex(verticesEmbedding.getUnchecked(edgePattern.getSrcPos())) ||
                            !edgeEmbedding.hasVertex(verticesEmbedding.getUnchecked(edgePattern.getDestPos()))) {
                        return false;
                    }
                }
            }

            return computation.filter(reusableEmbedding) && computation.shouldExpand(reusableEmbedding);
        }

        public boolean getNextEnum(int targetSize) {

            while (!enumerationStack.isEmpty()) {
                EnumerationStep lastEnumerationStep = enumerationStack.pop();
                int domainOfLastEnumerationStep = enumerationStack.size();
                int wordIdOfLastEnumerationStep = lastEnumerationStep.wordId;

                if (wordIdOfLastEnumerationStep >= 0) {
                    reusableEmbedding.removeLastWord();
                }

                int domainWithPointers = enumerationStack.size() - 1;

                // we are in the first domain: Domain0 -> Domain0EnumerationStep
                // Need to increment index of first domain
                if (domainWithPointers == -1) {

                    Domain0EnumerationStep domain0EnumerationStep = (Domain0EnumerationStep) lastEnumerationStep;

                    int currentIndex = domain0EnumerationStep.index;

                    while (++currentIndex < domain0Keys.length) {
                        int wordId = domain0Keys[currentIndex];

                        if (domainOfLastEnumerationStep <= targetSize - 1) {
                            boolean invalid = false;

                            // If we couldn't add this word this means that the
                            // current partial embedding and all extensions are invalid
                            // so skip everything and return false since enumId was associated
                            // with an invalid embedding.
                            if (!tryAddWord(wordId)) {
                                invalid = true;
                                // Add word anyway. Embedding will be invalid with this word but it will be
                                // popped on the next iteration of the while
                                reusableEmbedding.addWord(wordId);
                            }

                            domain0EnumerationStep.index = currentIndex;
                            domain0EnumerationStep.wordId = wordId;
                            enumerationStack.push(domain0EnumerationStep);

                            if (invalid) {
                                report.pruned[domainOfLastEnumerationStep] += 1;
                                numSpuriousEmbeddings += 1;
                                return false;
                            }
                            else {
                                report.explored[domainOfLastEnumerationStep] += 1;
                                // add new DomainNot0EnumerationStep with wordId = -1, and all possible ids for next domain
                                if (enumerationStack.size() != targetSize) {
                                    int[] nextDomainPointers = getWordIdsOfDomain(1);

                                    enumerationStack.push( new DomainNot0EnumerationStep(-1, nextDomainPointers) );
                                }

                                break;
                            }
                        }
                    }
                } // we are now in one of the non-0 domains: Domain0 -> DomainNot0EnumerationStep
                else {
                    DomainNot0EnumerationStep domainNot0EnumerationStep = (DomainNot0EnumerationStep) lastEnumerationStep;

                    int[] possibilitiesLastDomain;
                    if(domainOfLastEnumerationStep + 1 ==  numberOfDomains)
                        possibilitiesLastDomain = new int[0];
                    else
                        possibilitiesLastDomain = getWordIdsOfDomain(domainOfLastEnumerationStep + 1);

                    int[] pointers = domainNot0EnumerationStep.domain;

                    for (int i = domainNot0EnumerationStep.pos + 1; i < pointers.length; i++) {
                        int newWordId = pointers[i];

                        if (domainOfLastEnumerationStep <= targetSize - 1) {
                            boolean invalid = false;

                            // If we couldn't add this word this means that the
                            // current partial embedding and all extensions are invalid
                            // so skip everything and return false since enumId was associated
                            // with an invalid embedding.
                            if (!tryAddWord(newWordId)) {
                                invalid = true;
                                // Add word anyway. Embedding will be invalid with this word but it will be
                                // popped on the next iteration of the while
                                reusableEmbedding.addWord(newWordId);
                            }

                            lastEnumerationStep.wordId = newWordId;
                            ((DomainNot0EnumerationStep) lastEnumerationStep).pos = i;
                            enumerationStack.push(lastEnumerationStep);

                            if (invalid) {
                                numSpuriousEmbeddings += 1;
                                report.pruned[domainOfLastEnumerationStep] += 1;

                                return false;
                            } else {
                                report.explored[domainOfLastEnumerationStep] += 1;

                                if (enumerationStack.size() != targetSize)
                                    enumerationStack.push( new DomainNot0EnumerationStep(-1, possibilitiesLastDomain) );

                                break;
                            }
                        }
                    }
                }

                if (enumerationStack.size() == targetSize) // If enumeration stack is of the desired size
                    if (enumerationStack.peek().wordId >= 0) // And last element actually represents a valid element
                        break; // Get out of the loop
            }

            numCompleteEnumerationsVisited += 1;
            boolean isCompleteEmbeddingValid = testCompleteEmbedding();
            boolean isEmbeddingOfTargetSize = reusableEmbedding.getNumWords() == targetSize;

            if(!(isCompleteEmbeddingValid && isEmbeddingOfTargetSize))
                numSpuriousEmbeddings += 1;

            return isEmbeddingOfTargetSize && isCompleteEmbeddingValid;
        }

        public String toStringResume() {
            StringBuilder sb = new StringBuilder();
            sb.append("EmbeddingsZip Reader:\n");
            sb.append("Enumerations: " + targetEnumId + " " + numberOfEnumerations + "\n");
            return sb.toString();
        }

        public boolean moveNext() {
            while(!enumerationStack.isEmpty())
                if(getNextEnum(domainEntries.size()))
                    return true;
            return false;
        }

        @Override
        public void close() {
            // Do nothing by default
        }

        public abstract class EnumerationStep {
            int wordId;

            public EnumerationStep(int wordId) {
                this.wordId = wordId;
            }

            @Override
            public String toString() {
                return "EnumerationStep{" +
                        "wordId=" + wordId +
                        '}';
            }
        }

        public class Domain0EnumerationStep extends EnumerationStep {
            int index;

            public Domain0EnumerationStep(int wordId, int index) {
                super(wordId);
                this.index = index;
            }

            @Override
            public String toString() {
                return "Domain0EnumerationStep{" +
                        "index=" + index +
                        "} " + super.toString();
            }
        }

        public class DomainNot0EnumerationStep extends EnumerationStep {
            int[] domain;//IntCursor cursor;
            int pos = -1;

            public DomainNot0EnumerationStep(int wordId, int[] domain) {
                super(wordId);
                this.domain = domain;
            }

            @Override
            public String toString() {
                return "DomainNot0EnumerationStep{" +
                        "cursor=" + domain +
                        "} " + super.toString();
            }
        }

        private class EdgesConsumer extends IntCollectionAddConsumer {
            private boolean hasLabel;
            private int targetLabel;

            public EdgesConsumer(boolean hasLabel) {
                this.hasLabel = hasLabel;
            }

            public void setPatternEdge(PatternEdge patternEdge) {
                if (hasLabel) {
                    this.targetLabel = ((LabelledPatternEdge) patternEdge).getLabel();
                }
            }

            @Override
            public void accept(int edgeId) {
                if (hasLabel) {
                    LabelledEdge labelledEdge = (LabelledEdge) mainGraph.getEdge(edgeId);

                    if (labelledEdge.getEdgeLabel() != targetLabel) {
                        return;
                    }
                }

                super.accept(edgeId);
            }
        }
    }
}
