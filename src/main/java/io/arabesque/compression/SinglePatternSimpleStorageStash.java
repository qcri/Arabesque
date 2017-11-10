package io.arabesque.compression;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

public class SinglePatternSimpleStorageStash extends SimpleStorageStash<SinglePatternSimpleStorage,SinglePatternSimpleStorageStash> implements Externalizable {
    private static final Logger LOG =
            Logger.getLogger(SinglePatternSimpleStorageStash.class);

    private Map<Pattern, SinglePatternSimpleStorage> compressedEmbeddingsByPattern;
    private Pattern reusablePattern;

    public SinglePatternSimpleStorageStash() {
        this (new HashMap<Pattern,SinglePatternSimpleStorage>());
    }

    public SinglePatternSimpleStorageStash(Map<Pattern,SinglePatternSimpleStorage> odagsByPattern) {
        this.compressedEmbeddingsByPattern = odagsByPattern;
        this.reusablePattern = Configuration.get().createPattern();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        try {
            reusablePattern.setEmbedding(embedding);
            SinglePatternSimpleStorage embeddingsZip = compressedEmbeddingsByPattern.get(reusablePattern);

            if (embeddingsZip == null) {
                Pattern patternCopy = reusablePattern.copy();
                embeddingsZip = new SinglePatternSimpleStorage(patternCopy, embedding.getNumWords());
                compressedEmbeddingsByPattern.put(patternCopy, embeddingsZip);
            }

            embeddingsZip.addEmbedding(embedding);
        } catch (Exception e) {
            LOG.error("Error adding embedding to simple stash", e);
            LOG.error("Embedding: " + embedding);
            LOG.error("Pattern: " + reusablePattern);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void aggregate(SinglePatternSimpleStorage ezip) {
        Pattern pattern = ezip.getPattern();

        SinglePatternSimpleStorage existingEzip = compressedEmbeddingsByPattern.get(pattern);

        if (existingEzip == null) // this is a new pattern storage
            compressedEmbeddingsByPattern.put(pattern, ezip);
        else // the pattern already exists in the stash, so aggregate it to the corresponding storage
            existingEzip.aggregate(ezip);
    }

    @Override
    public void aggregateUsingReusable(SinglePatternSimpleStorage ezip) {
        Pattern pattern = ezip.getPattern();

        SinglePatternSimpleStorage existingEzip = compressedEmbeddingsByPattern.get(pattern);

        if (existingEzip == null) { // this is a new pattern storage
            Pattern patternCopy = pattern.copy();
            ezip.setPattern(patternCopy);
            existingEzip = new SinglePatternSimpleStorage(patternCopy, ezip.getNumberOfDomains());
            compressedEmbeddingsByPattern.put(patternCopy, existingEzip);
        }

        existingEzip.aggregate(ezip);
    }


    @Override
    public void aggregateStash(SinglePatternSimpleStorageStash value) {
        for (Map.Entry<Pattern, SinglePatternSimpleStorage> otherCompressedEmbeddingsByPatternEntry :
                value.compressedEmbeddingsByPattern.entrySet()) {
            Pattern pattern = otherCompressedEmbeddingsByPatternEntry.getKey();
            SinglePatternSimpleStorage otherCompressedEmbeddings = otherCompressedEmbeddingsByPatternEntry.getValue();

            SinglePatternSimpleStorage thisCompressedEmbeddings = compressedEmbeddingsByPattern.get(pattern);

            if (thisCompressedEmbeddings == null)
                compressedEmbeddingsByPattern.put(pattern, otherCompressedEmbeddings);
            else
                thisCompressedEmbeddings.aggregate(otherCompressedEmbeddings);
        }
    }

    @Override
    public void finalizeConstruction(ExecutorService pool, int parts) {
       for (Map.Entry<Pattern, SinglePatternSimpleStorage> entry : compressedEmbeddingsByPattern.entrySet()) {
          Pattern pattern = entry.getKey();
          SinglePatternSimpleStorage simpleStorage = entry.getValue();
           simpleStorage.setPattern (pattern);
           simpleStorage.finalizeConstruction(pool, parts);
       }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(compressedEmbeddingsByPattern.size());
        for (Map.Entry<Pattern, SinglePatternSimpleStorage> shrunkEmbeddingsByPatternEntry :
                compressedEmbeddingsByPattern.entrySet()) {
            Pattern pattern = shrunkEmbeddingsByPatternEntry.getKey();
            pattern.write(dataOutput);
            SinglePatternSimpleStorage shrunkEmbeddings = shrunkEmbeddingsByPatternEntry.getValue();
            shrunkEmbeddings.write(dataOutput);
        }
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write(objOutput);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        compressedEmbeddingsByPattern.clear();
        int numEntries = dataInput.readInt();
        for (int i = 0; i < numEntries; ++i) {
            Pattern pattern = Configuration.get().createPattern();
            pattern.readFields(dataInput);
            SinglePatternSimpleStorage shrunkEmbeddings = new SinglePatternSimpleStorage(false);
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

    public SinglePatternSimpleStorage getEzip(Pattern pattern) {
        return compressedEmbeddingsByPattern.get(pattern);
    }

    public static class Aggregator extends BasicAggregator<SinglePatternSimpleStorageStash> {
        @Override
        public void aggregate(SinglePatternSimpleStorageStash value) {
            getAggregatedValue().aggregateStash(value);
        }

        @Override
        public SinglePatternSimpleStorageStash createInitialValue() {
            return new SinglePatternSimpleStorageStash();
        }
    }

    @Override
    public Collection<SinglePatternSimpleStorage> getEzips() {
        return compressedEmbeddingsByPattern.values();
    }

    @Override
    public String toString() {
        return "SinglePatternSimpleStorageStash{" +
                "compressedEmbeddingsByPattern=" + compressedEmbeddingsByPattern +
                '}';
    }

    public String toStringResume() {
        long numDomainsZips = 0;

        long numDomainsEnumerations = 0;

        for (SinglePatternSimpleStorage ezip : compressedEmbeddingsByPattern.values()) {
            ++numDomainsZips;
            numDomainsEnumerations += ezip.getNumberOfEnumerations();
        }

        return "SinglePatternSimpleStorageStash{" +
                "numZips=" + numDomainsZips + ", " +
                "numEnumerations=" + numDomainsEnumerations + ", " +
                "}";
    }

    public String toStringDebug() {
        StringBuilder sb = new StringBuilder();

        //TreeMap<String, EmbeddingsZip> orderedMap = new TreeMap<>();
        TreeMap<String, SinglePatternSimpleStorage> orderedMap = new TreeMap<>();

        //for (Map.Entry<Pattern, EmbeddingsZip> entry : compressedEmbeddingsByPattern.entrySet()) {
        for (Map.Entry<Pattern, SinglePatternSimpleStorage> entry : compressedEmbeddingsByPattern.entrySet()) {
            orderedMap.put(entry.getKey().toString(), entry.getValue());
        }

        sb.append("SinglePatternSimpleStorageStash{\n");

        int totalSum = 0;

        for (Map.Entry<String, SinglePatternSimpleStorage> entry : orderedMap.entrySet()) {
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

        for (SinglePatternSimpleStorage ezip : compressedEmbeddingsByPattern.values()) {
            domainStorageStats.aggregate(ezip.getStats());
        }

        return domainStorageStats.toString() + "\n" + domainStorageStats.getSizeEstimations();
    }
}
