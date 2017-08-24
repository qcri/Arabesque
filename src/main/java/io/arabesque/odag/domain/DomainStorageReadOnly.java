package io.arabesque.odag.domain;

import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.Vertex;
import io.arabesque.graph.LabelledEdge;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.LabelledPatternEdge;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.PatternEdge;
import io.arabesque.pattern.PatternEdgeArrayList;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import io.arabesque.report.StorageReport;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

import scala.Int;
import scala.collection.JavaConversions.*;

public class DomainStorageReadOnly extends DomainStorage {
    private static final Logger LOG = Logger.getLogger(DomainEntryReadOnly.class);

    @Override
    public void readFields(DataInput dataInput) throws IOException { 
        this.clear();

        numEmbeddings = dataInput.readLong();
        setNumberOfDomains(dataInput.readInt());

        for (int i = 0; i < numberOfDomains; ++i) {
            int domainEntryMapSize = dataInput.readInt();

            ConcurrentHashMap<Integer, DomainEntry> domainEntryMap = domainEntries.get(i);

            for (int j = 0; j < domainEntryMapSize; ++j) {
                int wordId = dataInput.readInt();

                DomainEntrySet domainEntry = new DomainEntryReadOnly();
                domainEntry.readFields(dataInput);

                domainEntryMap.put(wordId, domainEntry);
            }
        }
        countsDirty = true;
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
        return new MultiPatternReader(patterns, computation, numPartitions, numBlocks, maxBlockSize);
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

        private long targetEnumId;

        private EdgesConsumer edgesConsumer;
        private IntArrayList edgeIds;

        private final boolean debugCtor = false;

        protected StorageReport report = new StorageReport();
        protected long numCompleteEnumerationsVisited = 0;
        // how many invalid embeddings this storage/partition generated
        protected long numSpuriousEmbeddings = 0L;

        public Reader(Pattern pattern, Computation<Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
            this.pattern = pattern;
            this.computation = computation;
            this.numPartitions = numPartitions;
            mainGraph = Configuration.get().getMainGraph();
            reusableEmbedding = Configuration.get().createEmbedding();

            this.numberOfEnumerations = getNumberOfEnumerations();

            this.blockSize = Math.min(Math.max(numberOfEnumerations / numBlocks, 1L), maxBlockSize);

            enumerationStack = new ArrayDeque<>();
            enumerationStack.add(new Domain0EnumerationStep(0, -1, -1));

            singletonExtensionSet = HashIntSets.newMutableSet(1);

            partitionId = computation.getPartitionId();

            targetEnumId = -1;

            edgeIds = new IntArrayList();

            edgesConsumer = new EdgesConsumer(Configuration.get().isGraphEdgeLabelled());
            edgesConsumer.setCollection(edgeIds);

            if(debugCtor && computation.getStep() >= 2) {
                System.out.println("\nInside ctor(partitionId=" + partitionId + " in SuperStep(" + computation.getStep() + " of SimpleDomainStorageReadOnly.Reader with \n{" +
                        "\nnumberOfEnumerations=" + numberOfEnumerations +
                        "\nnumberOfPartitions=" + numPartitions +
                        "\nnumberOfBlocks=" + numBlocks +
                        "\nmaxBlockSize=" + maxBlockSize +
                        "\nblockSize=" + this.blockSize +
                        "\n}");
            }

            report.initReport(numberOfDomains);
        }

        protected void finalizeReport() {
            report.setNumEnumerations(numberOfEnumerations);
            report.setNumCompleteEnumerationsVisited(numCompleteEnumerationsVisited);
            report.setNumSpuriousEmbeddings(numSpuriousEmbeddings);
            report.setNumActualEmbeddings(numEmbeddings);

            for(int i = 0; i < numberOfDomains; ++i) {
                report.setDomainSize(i, domainEntries.get(i).size());
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

                int numVertices = reusableVertexEmbedding.getNumVertices();
                IntArrayList vertices = reusableVertexEmbedding.getVertices();

                for (int i = 0; i < numVertices; ++i) {
                    int vertexId = vertices.getUnchecked(i);

                    // Trying to add existing vertex
                    if (wordId == vertexId) {
                        return false;
                    }
                }

                singletonExtensionSet.clear();
                singletonExtensionSet.add(wordId);

                computation.filter(reusableEmbedding, singletonExtensionSet);

                if (singletonExtensionSet.size() == 0) {
                    return false;
                }

                if (!computation.filter(reusableVertexEmbedding, wordId)) {
                    return false;
                }

                reusableVertexEmbedding.addWord(wordId);
            } else if (reusableEmbedding instanceof EdgeInducedEmbedding) {
                EdgeInducedEmbedding reusableEdgeEmbedding = (EdgeInducedEmbedding) reusableEmbedding;

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
                if (!edgeIds.contains(wordId)) {
                    return false;
                }

                if (!computation.filter(reusableEdgeEmbedding, wordId)) {
                    return false;
                }

                reusableEdgeEmbedding.addWord(wordId);
            } else {
                throw new RuntimeException("Incompatible embedding class: " + reusableEmbedding.getClass());
            }

            return true;
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

        private IntCollection getEdgeIds(int srcId, int dstId, PatternEdge patternEdge) {
            edgeIds.clear();
            edgesConsumer.setPatternEdge(patternEdge);

            mainGraph.forEachEdgeId(srcId, dstId, edgesConsumer);

            return edgeIds;
        }

        private boolean testCompleteEmbedding() {
            if (reusableEmbedding.getNumVertices() != pattern.getNumberOfVertices()) {
                return false;
            }

            if (reusableEmbedding instanceof VertexInducedEmbedding) {
                VertexInducedEmbedding reusableVertexEmbedding = (VertexInducedEmbedding) reusableEmbedding;

                // Test if constructed embedding matches the pattern.
                // TODO: Perhaps we can do this incrementally in an efficient manner?
                int numEdgesPattern = pattern.getNumberOfEdges();
                int numEdgesEmbedding = reusableVertexEmbedding.getNumEdges();

                if (numEdgesEmbedding != numEdgesPattern) {
                    return false;
                }

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

        public boolean getEnumerationWithStack(int targetSize) {
            long currentId = 0;

            while (!enumerationStack.isEmpty() && targetEnumId >= currentId) {
                EnumerationStep lastEnumerationStep = enumerationStack.pop();

                int domainOfLastEnumerationStep = enumerationStack.size();
                int wordIdOfLastEnumerationStep = lastEnumerationStep.wordId;
                currentId = lastEnumerationStep.currentId;

                if (wordIdOfLastEnumerationStep >= 0) {
                    currentId += domainEntries.get(domainOfLastEnumerationStep).get(wordIdOfLastEnumerationStep).getCounter();
                    reusableEmbedding.removeLastWord();
                }

                int domainWithPointers = enumerationStack.size() - 1;

                // we are in the first domain: Domain0 -> Domain0EnumerationStep
                // Need to increment index of first domain
                if (domainWithPointers == -1) {
                    Domain0EnumerationStep domain0EnumerationStep = (Domain0EnumerationStep) lastEnumerationStep;

                    int currentIndex = domain0EnumerationStep.index;

                    ConcurrentHashMap<Integer, DomainEntry> domain0 = domainEntries.get(0);

                    while (++currentIndex < domain0OrderedKeys.length) {
                        int wordId = domain0OrderedKeys[currentIndex];
                        DomainEntry newPossibilityForDomain0 = domain0.get(wordId);

                        if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForDomain0.getCounter() > targetEnumId)
                                || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
                            boolean invalid = false;

                            // If we couldn't add this word this means that the
                            // current partial embedding and all extensions are invalid
                            // so skip everything and return false since enumId was associated
                            // with an invalid embedding.
                            if (!tryAddWord(wordId)) {
                                targetEnumId = currentId + newPossibilityForDomain0.getCounter() - 1;
                                invalid = true;
                                // Add word anyway. Embedding will be invalid with this word but it will be
                                // popped on the next iteration of the while
                                reusableEmbedding.addWord(wordId);
                            }

                            domain0EnumerationStep.index = currentIndex;
                            domain0EnumerationStep.currentId = currentId;
                            domain0EnumerationStep.wordId = wordId;
                            enumerationStack.push(domain0EnumerationStep);

                            if (invalid) {
                                numSpuriousEmbeddings += 1;
                                report.incrementPruned(domainOfLastEnumerationStep,1);
                                return false;
                            } else {
                                report.incrementExplored(domainOfLastEnumerationStep,1);
                                // add new DomainNot0EnumerationStep with wordId = -1, and all possible ids for next domain
                                if (enumerationStack.size() != targetSize) {
                                    final DomainEntryReadOnly oneee = (DomainEntryReadOnly) newPossibilityForDomain0;

                                    enumerationStack.push(
                                            new DomainNot0EnumerationStep(currentId, -1,
                                                    oneee.getPointers()));
                                }

                                break;
                            }
                        }

                        currentId += newPossibilityForDomain0.getCounter();
                    }
                } // we are now in one of the non-0 domains: Domain0 -> DomainNot0EnumerationStep
                else {
                    DomainNot0EnumerationStep domainNot0EnumerationStep = (DomainNot0EnumerationStep) lastEnumerationStep;

                    ConcurrentHashMap<Integer, DomainEntry> possibilitiesLastDomain = domainEntries.get(domainOfLastEnumerationStep);

                    int[] pointers = domainNot0EnumerationStep.domain;

                    for (int i = domainNot0EnumerationStep.pos + 1; i < pointers.length; i++) {
                        int newWordId = pointers[i];

                        DomainEntry newPossibilityForLastDomain = possibilitiesLastDomain.get(newWordId);

                        assert newPossibilityForLastDomain != null;

                        if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForLastDomain.getCounter() > targetEnumId)
                                || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
                            boolean invalid = false;

                            // If we couldn't add this word this means that the
                            // current partial embedding and all extensions are invalid
                            // so skip everything and return false since enumId was associated
                            // with an invalid embedding.
                            if (!tryAddWord(newWordId)) {
                                targetEnumId = currentId + newPossibilityForLastDomain.getCounter() - 1;
                                invalid = true;
                                // Add word anyway. Embedding will be invalid with this word but it will be
                                // popped on the next iteration of the while
                                reusableEmbedding.addWord(newWordId);
                            }

                            lastEnumerationStep.currentId = currentId;
                            lastEnumerationStep.wordId = newWordId;
                            ((DomainNot0EnumerationStep) lastEnumerationStep).pos = i;
                            enumerationStack.push(lastEnumerationStep);

                            if (invalid) {
                                numSpuriousEmbeddings += 1;
                                report.incrementPruned(domainOfLastEnumerationStep,1);
                                return false;
                            } else {
                                report.incrementExplored(domainOfLastEnumerationStep,1);
                                if (enumerationStack.size() != targetSize) {
                                    final DomainEntryReadOnly oneee = (DomainEntryReadOnly) newPossibilityForLastDomain;
                                    enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers()));
                                }

                                break;
                            }
                        }

                        currentId += newPossibilityForLastDomain.getCounter();
                    }
                }

                // If enumeration stack is of the desired size
                if (enumerationStack.size() == targetSize) {
                    // And last element actually represents a valid element
                    if (enumerationStack.peek().wordId >= 0) {
                        // Get out of the loop
                        break;
                    }
                }
            }

            numCompleteEnumerationsVisited += 1;
            boolean isCompleteEmbeddingValid = testCompleteEmbedding();
            boolean isEmbeddingOfTargetSize = reusableEmbedding.getNumWords() == targetSize;

            if(!(isCompleteEmbeddingValid && isEmbeddingOfTargetSize))
                numSpuriousEmbeddings += 1;

            //return reusableEmbedding.getNumWords() == targetSize && testCompleteEmbedding();
            return isEmbeddingOfTargetSize && isCompleteEmbeddingValid;
        }

        public String toStringResume() {
            StringBuilder sb = new StringBuilder();
            sb.append("EmbeddingsZip Reader:\n");
            sb.append("Enumerations: " + targetEnumId + " " + numberOfEnumerations + "\n");
            return sb.toString();
        }

        public boolean moveNext() {
            while (true) {
                targetEnumId = getNextEnumerationId(targetEnumId);

                if (targetEnumId == -1) {
                    return false;
                }

                if (getEnumerationWithStack(domainEntries.size())) {
                    return true;
                }
            }
        }

        public long getNextEnumerationId(long enumId) {
            while (enumId < numberOfEnumerations - 1) {
                enumId++;

                long currentBlockId = enumId / blockSize;

                if (isThisMyBlock(currentBlockId)) {
                    return enumId;
                } else {
                    // -1 because we'll increment it at the beginning of the next iteration
                    enumId = (currentBlockId + blocksToSkip(currentBlockId)) * blockSize - 1;
                }
            }

            if (enumId >= numberOfEnumerations - 1) {
                enumId = -1;
            }

            return enumId;
        }

        public int blocksToSkip(long blockId) {
            int owningPartition = (int) (blockId % numPartitions);
            int myPartition = partitionId;

            if (myPartition < owningPartition) {
                myPartition += numPartitions;
            }

            return myPartition - owningPartition;
        }

        // means this enum is in my block to handle
        public boolean isThisMyBlock(long blockId) {
            return blockId % numPartitions == partitionId;
        }

        @Override
        public void close() {
            // Do nothing by default
        }

        public abstract class EnumerationStep {
            long currentId;
            int wordId;

            public EnumerationStep(long currentId, int wordId) {
                this.currentId = currentId;
                this.wordId = wordId;
            }

            @Override
            public String toString() {
                return "EnumerationStep{" +
                        "currentId=" + currentId +
                        ", wordId=" + wordId +
                        '}';
            }
        }

        public class Domain0EnumerationStep extends EnumerationStep {
            int index;

            public Domain0EnumerationStep(long currentId, int wordId, int index) {
                super(currentId, wordId);
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

            public DomainNot0EnumerationStep(long currentId, int wordId, int[] domain) {
                super(currentId, wordId);
                this.domain = domain;
            }

            @Override
            public String toString() {
                return "DomainNot0EnumerationStep{" +
                        "cursor=" + domain +
                        "} " + super.toString();
            }
        }
    }

    /**
     * @Experimental
     * This reader is meant to work with single and multi-pattern odags. In the
     * former case, we provide and array of patterns with the single pattern.
     * TODO: we could spend some time refactoring this class, its logic is
     * crucial to the system and yet it is very complicated.
     */
    public class MultiPatternReader implements StorageReader {

        private final MainGraph mainGraph;
        private final Embedding reusableEmbedding;
        private final Pattern reusablePattern;
        private final long numberOfEnumerations;

        private final long blockSize;
        private final int partitionId;

        private final Deque<EnumerationStep> enumerationStack;
        private final HashIntSet singletonExtensionSet;
        private final Pattern[] patterns;
        private final Computation<Embedding> computation;
        private final int numPartitions;

        private long targetEnumId;

        private EdgesConsumer edgesConsumer;
        private IntArrayList edgeIds;

        private long validEmbeddings;
        private long prunedByTheEnd;
        private long localEnumerations;
        private long numberOfEmbeddingsRead;

        public MultiPatternReader(Pattern[] patterns, Computation<Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
            this.patterns = patterns;
            this.computation = computation;
            this.numPartitions = numPartitions;
            mainGraph = Configuration.get().getMainGraph();
            reusableEmbedding = Configuration.get().createEmbedding();
            reusablePattern = Configuration.get().createPattern();

            this.numberOfEnumerations = getNumberOfEnumerations();

            this.blockSize = Math.min(Math.max(numberOfEnumerations / numBlocks, 1L), maxBlockSize);

            enumerationStack = new ArrayDeque<>();
            enumerationStack.add(new Domain0EnumerationStep(0, -1, -1));

            singletonExtensionSet = HashIntSets.newMutableSet(1);

            partitionId = computation.getPartitionId();

            targetEnumId = -1;

            edgeIds = new IntArrayList();

            localEnumerations = 0;
            validEmbeddings = 0;
            prunedByTheEnd = 0;
            numberOfEmbeddingsRead = 0;

            edgesConsumer = new EdgesConsumer(Configuration.get().isGraphEdgeLabelled());
            edgesConsumer.setCollection(edgeIds);
        }

        @Override
        public boolean hasNext() {
            return moveNext();
        }

        @Override
        public Embedding next() {
           numberOfEmbeddingsRead += 1; 
           return reusableEmbedding;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean tryAddWord(int wordId) {
            if (reusableEmbedding instanceof VertexInducedEmbedding) {
                VertexInducedEmbedding reusableVertexEmbedding = (VertexInducedEmbedding) reusableEmbedding;

                int numVertices = reusableVertexEmbedding.getNumVertices();
                IntArrayList vertices = reusableVertexEmbedding.getVertices();

                for (int i = 0; i < numVertices; ++i) {
                    int vertexId = vertices.getUnchecked(i);

                    // Trying to add existing vertex
                    if (wordId == vertexId) {
                        return false;
                    }
                }

                singletonExtensionSet.clear();
                singletonExtensionSet.add(wordId);

                computation.filter(reusableEmbedding, singletonExtensionSet);

                if (singletonExtensionSet.size() == 0) {
                    return false;
                }
                
                if (!computation.filter(reusableVertexEmbedding, wordId)) {
                    return false;
                }

                // incremental validation
                reusableVertexEmbedding.addWord(wordId);
                reusablePattern.setEmbedding (reusableVertexEmbedding);
                boolean validForSomePattern = false;
                for (Pattern pattern: patterns) {
                   if (!reusablePattern.equals(pattern, reusablePattern.getNumberOfEdges()))
                      continue;

                   validForSomePattern = true;
                   break;
                }

                if (!validForSomePattern){
                   reusableVertexEmbedding.removeLastWord();
                   return false;
                }


                // final add word
                //reusableVertexEmbedding.addWord(wordId);

            } else if (reusableEmbedding instanceof EdgeInducedEmbedding) {
                EdgeInducedEmbedding reusableEdgeEmbedding = (EdgeInducedEmbedding) reusableEmbedding;

                singletonExtensionSet.clear();
                singletonExtensionSet.add(wordId);

                computation.filter(reusableEmbedding, singletonExtensionSet);

                boolean validForSomePattern = false;
                for (Pattern pattern: patterns) {

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
                      continue;
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
                   if (!edgeIds.contains(wordId)) {
                      continue;
                   }

                   if (!computation.filter(reusableEdgeEmbedding, wordId)) {
                      continue;
                   }
                   validForSomePattern = true;
                   break;
                }

                if (!validForSomePattern) return false;
                reusableEdgeEmbedding.addWord(wordId);
            } else {
                throw new RuntimeException("Incompatible embedding class: " + reusableEmbedding.getClass());
            }

            return true;
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

        private IntCollection getEdgeIds(int srcId, int dstId, PatternEdge patternEdge) {
            edgeIds.clear();
            edgesConsumer.setPatternEdge(patternEdge);

            mainGraph.forEachEdgeId(srcId, dstId, edgesConsumer);

            return edgeIds;
        }

        private boolean testCompleteEmbedding() {
            // NOTE: the following check is required when running in
            // multi-pattern odag mode. The issue is that althought
            // *aggregationFilter* was already performed at this point we may
            // have chose there that the odag must be kept for other pattern but
            // the ones being filtered. Thus some odags will carry invalid
            // pointers with vertice labels that lead to invalid patterns.
            if (!computation.aggregationFilter(reusableEmbedding.getPattern()))
               return false;

            boolean validForSomePattern = false;
            for (Pattern pattern: patterns) {
               if (reusableEmbedding.getNumVertices() != pattern.getNumberOfVertices()) {
                  continue;
               } else {
                  reusablePattern.setEmbedding (reusableEmbedding);
                  if (!reusablePattern.equals(pattern))
                     continue;
                   validForSomePattern = true;
                   break;
               }
            }

            if (!validForSomePattern) return false;

            if (reusableEmbedding instanceof VertexInducedEmbedding) {
                VertexInducedEmbedding reusableVertexEmbedding = (VertexInducedEmbedding) reusableEmbedding;

                validForSomePattern = false;
                for (Pattern pattern: patterns) {
                   // Test if constructed embedding matches the pattern.
                   // TODO: Perhaps we can do this incrementally in an efficient manner?
                   int numEdgesPattern = pattern.getNumberOfEdges();
                   int numEdgesEmbedding = reusableVertexEmbedding.getNumEdges();

                   if (numEdgesEmbedding != numEdgesPattern) {
                      continue;
                   }
                   
                   reusablePattern.setEmbedding (reusableEmbedding);
                   if (!reusablePattern.equals(pattern))
                      continue;

                   validForSomePattern = true;
                   break;

                }
                if (!validForSomePattern) {
                   return false;
                }
            }

            boolean valid = computation.filter(reusableEmbedding) && 
               computation.shouldExpand(reusableEmbedding);

            if (!valid)
               prunedByTheEnd += 1;

            return valid;
        }

        public boolean getEnumerationWithStack(int targetSize) {
            localEnumerations += 1;
            long currentId = 0;

            while (!enumerationStack.isEmpty() && targetEnumId >= currentId) {
                EnumerationStep lastEnumerationStep = enumerationStack.pop();

                int domainOfLastEnumerationStep = enumerationStack.size();
                int wordIdOfLastEnumerationStep = lastEnumerationStep.wordId;
                currentId = lastEnumerationStep.currentId;

                if (wordIdOfLastEnumerationStep >= 0) {
                    currentId += domainEntries.get(domainOfLastEnumerationStep).get(wordIdOfLastEnumerationStep).getCounter();
                    reusableEmbedding.removeLastWord();
                }

                int domainWithPointers = enumerationStack.size() - 1;

                // Need to increment index of first domain
                if (domainWithPointers == -1) {
                    Domain0EnumerationStep domain0EnumerationStep = (Domain0EnumerationStep) lastEnumerationStep;

                    int currentIndex = domain0EnumerationStep.index;

                    ConcurrentHashMap<Integer, DomainEntry> domain0 = domainEntries.get(0);

                    while (++currentIndex < domain0OrderedKeys.length) {
                        int wordId = domain0OrderedKeys[currentIndex];
                        DomainEntry newPossibilityForDomain0 = domain0.get(wordId);

                        if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForDomain0.getCounter() > targetEnumId)
                                || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
                            boolean invalid = false;

                            // If we couldn't add this word this means that the
                            // current partial embedding and all extensions are invalid
                            // so skip everything and return false since enumId was associated
                            // with an invalid embedding.
                            if (!tryAddWord(wordId)) {
                                targetEnumId = currentId + newPossibilityForDomain0.getCounter() - 1;
                                invalid = true;
                                // Add word anyway. Embedding will be invalid with this word but it will be
                                // popped on the next iteration of the while
                                reusableEmbedding.addWord(wordId);
                            }

                            domain0EnumerationStep.index = currentIndex;
                            domain0EnumerationStep.currentId = currentId;
                            domain0EnumerationStep.wordId = wordId;
                            enumerationStack.push(domain0EnumerationStep);

                            if (invalid) {
                                return false;
                            } else {
                                if (enumerationStack.size() != targetSize) {
                                    final DomainEntryReadOnly oneee = (DomainEntryReadOnly) newPossibilityForDomain0;

                                    enumerationStack.push(
                                            new DomainNot0EnumerationStep(currentId, -1,
                                                    oneee.getPointers()));
                                }

                                break;
                            }
                        }

                        currentId += newPossibilityForDomain0.getCounter();
                    }
                } else {
                    DomainNot0EnumerationStep domainNot0EnumerationStep = (DomainNot0EnumerationStep) lastEnumerationStep;

                    ConcurrentHashMap<Integer, DomainEntry> possibilitiesLastDomain = domainEntries.get(domainOfLastEnumerationStep);

                    int[] pointers = domainNot0EnumerationStep.domain;

                    for (int i = domainNot0EnumerationStep.pos + 1; i < pointers.length; i++) {
                        int newWordId = pointers[i];

                        DomainEntry newPossibilityForLastDomain = possibilitiesLastDomain.get(newWordId);
                        
                        assert newPossibilityForLastDomain != null;

                        if ((domainOfLastEnumerationStep < targetSize - 1 && currentId + newPossibilityForLastDomain.getCounter() > targetEnumId)
                                || (domainOfLastEnumerationStep == targetSize - 1 && currentId == targetEnumId)) {
                            boolean invalid = false;
                           
                            // If we couldn't add this word this means that the
                            // current partial embedding and all extensions are invalid
                            // so skip everything and return false since enumId was associated
                            // with an invalid embedding.
                            if (!tryAddWord(newWordId)) {
                                targetEnumId = currentId + newPossibilityForLastDomain.getCounter() - 1;
                                invalid = true;
                                // Add word anyway. Embedding will be invalid with this word but it will be
                                // popped on the next iteration of the while
                                reusableEmbedding.addWord(newWordId);
                            }

                            lastEnumerationStep.currentId = currentId;
                            lastEnumerationStep.wordId = newWordId;
                            ((DomainNot0EnumerationStep) lastEnumerationStep).pos = i;
                            enumerationStack.push(lastEnumerationStep);

                            if (invalid) {
                                return false;
                            } else {
                                if (enumerationStack.size() != targetSize) {
                                    final DomainEntryReadOnly oneee = (DomainEntryReadOnly) newPossibilityForLastDomain;
                                    enumerationStack.push(new DomainNot0EnumerationStep(currentId, -1, oneee.getPointers()));
                                }

                                break;
                            }
                        }

                        currentId += newPossibilityForLastDomain.getCounter();
                    }
                }

                // If enumeration stack is of the desired size
                if (enumerationStack.size() == targetSize) {
                    // And last element actually represents a valid element
                    if (enumerationStack.peek().wordId >= 0) {
                        // Get out of the loop
                        break;
                    }
                }
            }
            

            boolean valid = reusableEmbedding.getNumWords() == targetSize &&
               testCompleteEmbedding();

            if (valid) {
               validEmbeddings += reusableEmbedding.getNumWords();
            }

            return valid;
        }

        public String toStringResume() {
            StringBuilder sb = new StringBuilder();
            sb.append("EmbeddingsZip Reader:\n");
            sb.append("Enumerations: " + targetEnumId + " " + numberOfEnumerations + "\n");
            return sb.toString();
        }

        public boolean moveNext() {
            while (true) {
                
                targetEnumId = getNextEnumerationId(targetEnumId);

                if (targetEnumId == -1) {
                   return false;
                }

                if (getEnumerationWithStack(domainEntries.size())) {
                    return true;
                }
            }
        }

        public long getNextEnumerationId(long enumId) {
            while (enumId < numberOfEnumerations - 1) {
                enumId++;

                long currentBlockId = enumId / blockSize;

                if (isThisMyBlock(currentBlockId)) {
                    return enumId;
                } else {
                    // -1 because we'll increment it at the beginning of the next iteration
                    enumId = (currentBlockId + blocksToSkip(currentBlockId)) * blockSize - 1;
                }
            }

            if (enumId >= numberOfEnumerations - 1) {
                enumId = -1;
            }

            return enumId;
        }

        public int blocksToSkip(long blockId) {
            int owningPartition = (int) (blockId % numPartitions);
            int myPartition = partitionId;

            if (myPartition < owningPartition) {
                myPartition += numPartitions;
            }

            return myPartition - owningPartition;
        }

        public boolean isThisMyBlock(long blockId) {
            return blockId % numPartitions == partitionId;
        }

        @Override
        public void close() {
            if (LOG.isDebugEnabled()) {
               LOG.debug ("numberOfEmbeddingsRead " + numberOfEmbeddingsRead);
               LOG.debug (Configuration.get().getCommStrategy() +
                  " prunedByTheEnd: " + prunedByTheEnd +
                  " validEmbeddings: " +
                  validEmbeddings/domainEntries.size() +
                  " enumerations: " + localEnumerations);
            }
        }

        public abstract class EnumerationStep {
            long currentId;
            int wordId;

            public EnumerationStep(long currentId, int wordId) {
                this.currentId = currentId;
                this.wordId = wordId;
            }

            @Override
            public String toString() {
                return "EnumerationStep{" +
                        "currentId=" + currentId +
                        ", wordId=" + wordId +
                        '}';
            }
        }

        public class Domain0EnumerationStep extends EnumerationStep {
            int index;

            public Domain0EnumerationStep(long currentId, int wordId, int index) {
                super(currentId, wordId);
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

            public DomainNot0EnumerationStep(long currentId, int wordId, int[] domain) {
                super(currentId, wordId);
                this.domain = domain;
            }

            @Override
            public String toString() {
                return "DomainNot0EnumerationStep{" +
                        "cursor=" + domain +
                        "} " + super.toString();
            }
        }
    }
}
