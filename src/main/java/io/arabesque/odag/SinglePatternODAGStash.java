package io.arabesque.odag;

import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class SinglePatternODAGStash extends BasicODAGStash<SinglePatternODAG, SinglePatternODAGStash> {
    private static final Logger LOG =
            Logger.getLogger(SinglePatternODAGStash.class);

    private Map<Pattern, SinglePatternODAG> compressedEmbeddingsByPattern;
    private Pattern reusablePattern;

    public SinglePatternODAGStash() {
        compressedEmbeddingsByPattern = new HashMap<>();
        this.reusablePattern = Configuration.get().createPattern();
    }

    public SinglePatternODAGStash(Map<Pattern, SinglePatternODAG> odagsByPattern) {
       compressedEmbeddingsByPattern = odagsByPattern;
       this.reusablePattern = Configuration.get().createPattern();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        try {
            reusablePattern.setEmbedding(embedding);
            SinglePatternODAG embeddingsZip = compressedEmbeddingsByPattern.get(reusablePattern);

            if (embeddingsZip == null) {
                Pattern patternCopy = reusablePattern.copy();
                embeddingsZip = new SinglePatternODAG(patternCopy, embedding.getNumWords());
                compressedEmbeddingsByPattern.put(patternCopy, embeddingsZip);
            }

            embeddingsZip.addEmbedding(embedding);
        } catch (Exception e) {
            LOG.error("Error adding embedding to odag stash", e);
            LOG.error("Embedding: " + embedding);
            LOG.error("Pattern: " + reusablePattern);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void aggregate(SinglePatternODAG ezip) {
        Pattern pattern = ezip.getPattern();

        SinglePatternODAG existingEzip = compressedEmbeddingsByPattern.get(pattern);

        if (existingEzip == null) {
            compressedEmbeddingsByPattern.put(pattern, ezip);
        } else {
            existingEzip.aggregate(ezip);
        }
    }

    @Override
    public void aggregateUsingReusable(SinglePatternODAG ezip) {
        Pattern pattern = ezip.getPattern();

        SinglePatternODAG existingEzip = compressedEmbeddingsByPattern.get(pattern);

        if (existingEzip == null) {
            Pattern patternCopy = pattern.copy();
            ezip.setPattern(patternCopy);
            existingEzip = new SinglePatternODAG(patternCopy, ezip.getNumberOfDomains());
            compressedEmbeddingsByPattern.put(patternCopy, existingEzip);
        }

        existingEzip.aggregate(ezip);
    }


    @Override
    public void aggregate(SinglePatternODAGStash value) {
        for (Map.Entry<Pattern, SinglePatternODAG> otherCompressedEmbeddingsByPatternEntry :
                value.compressedEmbeddingsByPattern.entrySet()) {
            Pattern pattern = otherCompressedEmbeddingsByPatternEntry.getKey();
            SinglePatternODAG otherCompressedEmbeddings = otherCompressedEmbeddingsByPatternEntry.getValue();

            SinglePatternODAG thisCompressedEmbeddings = compressedEmbeddingsByPattern.get(pattern);

            if (thisCompressedEmbeddings == null) {
                compressedEmbeddingsByPattern.put(pattern, otherCompressedEmbeddings);
            } else {
                thisCompressedEmbeddings.aggregate(otherCompressedEmbeddings);
            }
        }
    }

    @Override
    public void finalizeConstruction(ExecutorService pool, int parts) {
       for (Map.Entry<Pattern, SinglePatternODAG> entry : compressedEmbeddingsByPattern.entrySet()) {
          Pattern pattern = entry.getKey();
          SinglePatternODAG odag = entry.getValue();
          odag.setPattern (pattern);
          odag.finalizeConstruction(pool, parts);
       }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(compressedEmbeddingsByPattern.size());
        for (Map.Entry<Pattern, SinglePatternODAG> shrunkEmbeddingsByPatternEntry :
                compressedEmbeddingsByPattern.entrySet()) {
            Pattern pattern = shrunkEmbeddingsByPatternEntry.getKey();
            pattern.write(dataOutput);
            SinglePatternODAG shrunkEmbeddings = shrunkEmbeddingsByPatternEntry.getValue();
            shrunkEmbeddings.write(dataOutput);
        }
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write(objOutput);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (true) {
            throw new RuntimeException("Shouldn't be used any more");
        }
        compressedEmbeddingsByPattern.clear();
        int numEntries = dataInput.readInt();
        for (int i = 0; i < numEntries; ++i) {
            Pattern pattern = Configuration.get().createPattern();
            pattern.readFields(dataInput);
            SinglePatternODAG shrunkEmbeddings = new SinglePatternODAG(false);
            shrunkEmbeddings.setPattern(pattern);
            shrunkEmbeddings.readFields(dataInput);
            compressedEmbeddingsByPattern.put(pattern, shrunkEmbeddings);
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields (objInput);
    }

    @Override
    public boolean isEmpty() {
        return compressedEmbeddingsByPattern.isEmpty();
    }

    @Override
    public int getNumZips() {
        return compressedEmbeddingsByPattern.size();
    }

    @Override
    public void clear() {
        compressedEmbeddingsByPattern.clear();
    }

    public SinglePatternODAG getEzip(Pattern pattern) {
        return compressedEmbeddingsByPattern.get(pattern);
    }

    public static class Aggregator extends BasicAggregator<SinglePatternODAGStash> {
        @Override
        public void aggregate(SinglePatternODAGStash value) {
            getAggregatedValue().aggregate(value);
        }

        @Override
        public SinglePatternODAGStash createInitialValue() {
            return new SinglePatternODAGStash();
        }
    }

    public Collection<SinglePatternODAG> getEzips() {
        return compressedEmbeddingsByPattern.values();
    }

    @Override
    public String toString() {
        return "SinglePatternODAGStash{" +
                "compressedEmbeddingsByPattern=" + compressedEmbeddingsByPattern +
                '}';
    }

    public String toStringResume() {
        long numDomainsZips = 0;

        long numDomainsEnumerations = 0;

        for (SinglePatternODAG ezip : compressedEmbeddingsByPattern.values()) {
            ++numDomainsZips;
            numDomainsEnumerations += ezip.getNumberOfEnumerations();
        }

        return "SinglePatternODAGStash{" +
                "numZips=" + numDomainsZips + ", " +
                "numEnumerations=" + numDomainsEnumerations + ", " +
                "}";
    }

    public String toStringDebug() {
        StringBuilder sb = new StringBuilder();

        //TreeMap<String, EmbeddingsZip> orderedMap = new TreeMap<>();
        TreeMap<String, SinglePatternODAG> orderedMap = new TreeMap<>();

        //for (Map.Entry<Pattern, EmbeddingsZip> entry : compressedEmbeddingsByPattern.entrySet()) {
        for (Map.Entry<Pattern, SinglePatternODAG> entry : compressedEmbeddingsByPattern.entrySet()) {
            orderedMap.put(entry.getKey().toString(), entry.getValue());
        }

        sb.append("SinglePatternODAGStash{\n");

        int totalSum = 0;

        //for (Map.Entry<String, EmbeddingsZip> entry : orderedMap.entrySet()) {
        for (Map.Entry<String, SinglePatternODAG> entry : orderedMap.entrySet()) {
            sb.append("=====\n");
            sb.append(entry.getKey());
            sb.append('\n');
            sb.append(entry.getValue().toString());
            sb.append('\n');

            totalSum += entry.getValue().getNumberOfEnumerations();
        }

        sb.append("Total sum=");
        sb.append(totalSum);
        sb.append("\n}");

        return sb.toString();
    }

    public String getDomainStorageStatsString() {
        StorageStats domainStorageStats = new StorageStats();

        for (SinglePatternODAG ezip : compressedEmbeddingsByPattern.values()) {
            domainStorageStats.aggregate(ezip.getStats());
        }

        return domainStorageStats.toString() + "\n" + domainStorageStats.getSizeEstimations();
    }

    public static class EfficientReader<O extends Embedding> implements Reader<O> {
        private final int numPartitions;
        private final SinglePatternODAGStash stash;
        private final Computation<Embedding> computation;
        private final int numBlocks;
        private final int maxBlockSize;

        private Iterator<Map.Entry<Pattern, SinglePatternODAG>> stashIterator;
        private StorageReader currentReader;
        private boolean currentPositionConsumed = true;

        public EfficientReader(SinglePatternODAGStash stash, Computation<? extends Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
            this.stash = stash;
            this.numPartitions = numPartitions;
            this.computation = (Computation<Embedding>) computation;
            this.numBlocks = numBlocks;
            this.maxBlockSize = maxBlockSize;

            stashIterator = stash.compressedEmbeddingsByPattern.entrySet().iterator();
            currentReader = null;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (currentReader == null) {
                    if (stashIterator.hasNext()) {
                        Map.Entry<Pattern, SinglePatternODAG> nextEntry = stashIterator.next();
                        currentReader = nextEntry.getValue().getReader(computation, numPartitions, numBlocks, maxBlockSize);
                    }
                }

                // No more zips, for sure nothing else to do
                if (currentReader == null) {
                    currentPositionConsumed = true;
                    return false;
                }

                // If we consumed the current embedding (called next after a previous hasNext),
                // we need to actually advance to the next one.
                if (currentPositionConsumed && currentReader.hasNext()) {
                    currentPositionConsumed = false;
                    return true;
                }
                // If we still haven't consumed the current embedding (called hasNext but haven't
                // called next), return the same result as before (which is necessarily true).
                else if (!currentPositionConsumed) {
                    return true;
                }
                // If we have consumed the current embedding and the current reader doesn't have
                // more embeddings, we need to advance to the next reader so set currentReader to
                // null and let the while begin again (simulate recursive call without the stack
                // building overhead).
                else {
                    currentReader.close();
                    currentReader = null;
                }
            }
        }

        @Override
        public O next() {
            currentPositionConsumed = true;

            return (O) currentReader.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
