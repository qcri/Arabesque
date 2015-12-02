package io.arabesque.odag;

import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class ODAGStash implements Writable {
    private static final Logger LOG =
            Logger.getLogger(ODAGStash.class);

    private Map<Pattern, ODAG> compressedEmbeddingsByPattern;
    private Pattern reusablePattern;

    public ODAGStash() {
        compressedEmbeddingsByPattern = new HashMap<>();
        this.reusablePattern = Configuration.get().createPattern();
    }

    public void addEmbedding(Embedding embedding) {
        try {
            reusablePattern.setEmbedding(embedding);
            ODAG embeddingsZip = compressedEmbeddingsByPattern.get(reusablePattern);

            if (embeddingsZip == null) {
                Pattern patternCopy = reusablePattern.copy();
                embeddingsZip = new ODAG(patternCopy, embedding.getNumWords());
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

    public void aggregate(ODAG ezip) {
        Pattern pattern = ezip.getPattern();

        ODAG existingEzip = compressedEmbeddingsByPattern.get(pattern);

        if (existingEzip == null) {
            compressedEmbeddingsByPattern.put(pattern, ezip);
        } else {
            existingEzip.aggregate(ezip);
        }
    }

    public void aggregateUsingReusable(ODAG ezip) {
        Pattern pattern = ezip.getPattern();

        ODAG existingEzip = compressedEmbeddingsByPattern.get(pattern);

        if (existingEzip == null) {
            Pattern patternCopy = pattern.copy();
            ezip.setPattern(patternCopy);
            existingEzip = new ODAG(patternCopy, ezip.getNumberOfDomains());
            compressedEmbeddingsByPattern.put(patternCopy, existingEzip);
        }

        existingEzip.aggregate(ezip);
    }


    public void aggregate(ODAGStash value) {
        for (Map.Entry<Pattern, ODAG> otherCompressedEmbeddingsByPatternEntry :
                value.compressedEmbeddingsByPattern.entrySet()) {
            Pattern pattern = otherCompressedEmbeddingsByPatternEntry.getKey();
            ODAG otherCompressedEmbeddings = otherCompressedEmbeddingsByPatternEntry.getValue();

            ODAG thisCompressedEmbeddings = compressedEmbeddingsByPattern.get(pattern);

            if (thisCompressedEmbeddings == null) {
                compressedEmbeddingsByPattern.put(pattern, otherCompressedEmbeddings);
            } else {
                thisCompressedEmbeddings.aggregate(otherCompressedEmbeddings);
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(compressedEmbeddingsByPattern.size());
        for (Map.Entry<Pattern, ODAG> shrunkEmbeddingsByPatternEntry :
                compressedEmbeddingsByPattern.entrySet()) {
            Pattern pattern = shrunkEmbeddingsByPatternEntry.getKey();
            pattern.write(dataOutput);
            ODAG shrunkEmbeddings = shrunkEmbeddingsByPatternEntry.getValue();
            shrunkEmbeddings.write(dataOutput);
        }
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
            ODAG shrunkEmbeddings = new ODAG(false);
            shrunkEmbeddings.setPattern(pattern);
            shrunkEmbeddings.readFields(dataInput);
            compressedEmbeddingsByPattern.put(pattern, shrunkEmbeddings);
        }
    }

    public boolean isEmpty() {
        return compressedEmbeddingsByPattern.isEmpty();
    }

    public int getNumZips() {
        return compressedEmbeddingsByPattern.size();
    }

    public void clear() {
        compressedEmbeddingsByPattern.clear();
    }

    public ODAG getEzip(Pattern pattern) {
        return compressedEmbeddingsByPattern.get(pattern);
    }

    public static class Aggregator extends BasicAggregator<ODAGStash> {
        @Override
        public void aggregate(ODAGStash value) {
            getAggregatedValue().aggregate(value);
        }

        @Override
        public ODAGStash createInitialValue() {
            return new ODAGStash();
        }
    }

    public Collection<ODAG> getEzips() {
        return compressedEmbeddingsByPattern.values();
    }

    @Override
    public String toString() {
        return "ODAGStash{" +
                "compressedEmbeddingsByPattern=" + compressedEmbeddingsByPattern +
                '}';
    }

    public String toStringResume() {
        long numDomainsZips = 0;

        long numDomainsEnumerations = 0;

        for (ODAG ezip : compressedEmbeddingsByPattern.values()) {
            ++numDomainsZips;
            numDomainsEnumerations += ezip.getNumberOfEnumerations();
        }

        return "ODAGStash{" +
                "numZips=" + numDomainsZips + ", " +
                "numEnumerations=" + numDomainsEnumerations + ", " +
                "}";
    }

    public String toStringDebug() {
        StringBuilder sb = new StringBuilder();

        //TreeMap<String, EmbeddingsZip> orderedMap = new TreeMap<>();
        TreeMap<String, ODAG> orderedMap = new TreeMap<>();

        //for (Map.Entry<Pattern, EmbeddingsZip> entry : compressedEmbeddingsByPattern.entrySet()) {
        for (Map.Entry<Pattern, ODAG> entry : compressedEmbeddingsByPattern.entrySet()) {
            orderedMap.put(entry.getKey().toString(), entry.getValue());
        }

        sb.append("ODAGStash{\n");

        int totalSum = 0;

        //for (Map.Entry<String, EmbeddingsZip> entry : orderedMap.entrySet()) {
        for (Map.Entry<String, ODAG> entry : orderedMap.entrySet()) {
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

        for (ODAG ezip : compressedEmbeddingsByPattern.values()) {
            domainStorageStats.aggregate(ezip.getStats());
        }

        return domainStorageStats.toString() + "\n" + domainStorageStats.getSizeEstimations();
    }

    public interface Reader<O extends Embedding> extends Iterator<O> {

    }

    public static class EfficientReader<O extends Embedding> implements Reader<O> {
        private final int numPartitions;
        private final ODAGStash stash;
        private final Computation<Embedding> computation;
        private final int numBlocks;
        private final int maxBlockSize;

        private Iterator<Map.Entry<Pattern, ODAG>> stashIterator;
        private StorageReader currentReader;
        private boolean currentPositionConsumed = true;

        public EfficientReader(ODAGStash stash, Computation<? extends Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
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
                        Map.Entry<Pattern, ODAG> nextEntry = stashIterator.next();
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
