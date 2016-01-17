package io.arabesque.odag.domain;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.WriterSetConsumer;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCursor;
import org.weakref.jmx.com.google.common.primitives.Ints;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DomainStorage extends Storage<DomainStorage> {
    protected boolean countsDirty;
    protected ArrayList<Map<Integer, DomainEntry>> domainEntries;
    protected int[] domain0OrderedKeys;
    protected int numberOfDomains;
    protected WriterSetConsumer writerSetConsumer;

    public DomainStorage(int numberOfDomains) {
        setNumberOfDomains(numberOfDomains);
        countsDirty = false;
        writerSetConsumer = new WriterSetConsumer();
    }

    public DomainStorage() {
        numberOfDomains = -1;
        countsDirty = false;
        writerSetConsumer = new WriterSetConsumer();
    }

    protected synchronized void setNumberOfDomains(int numberOfDomains) {
        if (numberOfDomains == this.numberOfDomains) {
            return;
        }

        ensureCanStoreNDomains(numberOfDomains);
        this.numberOfDomains = numberOfDomains;
    }

    private void ensureCanStoreNDomains(int nDomains) {
        if (nDomains < 0) {
            return;
        }

        if (domainEntries == null) {
            domainEntries = new ArrayList<>(nDomains);
        } else {
            domainEntries.ensureCapacity(nDomains);
        }

        int currentNumDomains = domainEntries.size();
        int delta = nDomains - currentNumDomains;

        for (int i = 0; i < delta; ++i) {
            domainEntries.add(new ConcurrentHashMap<Integer, DomainEntry>());
        }
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        int numWords = embedding.getNumWords();
        IntArrayList words = embedding.getWords();

        if (domainEntries.size() != numWords) {
            throw new RuntimeException("Tried to add an embedding with wrong number " +
                    "of expected vertices (" + domainEntries.size() + ") " + embedding);
        }

        for (int i = 0; i < numWords; ++i) {
            DomainEntry domainEntryForCurrentWord = domainEntries.get(i).get(words.getUnchecked(i));

            if (domainEntryForCurrentWord == null) {
                domainEntryForCurrentWord = new DomainEntrySet();
                domainEntries.get(i).put(words.getUnchecked(i), domainEntryForCurrentWord);
            }
        }

        for (int i = numWords - 1; i > 0; --i) {
            DomainEntry domainEntryForPreviousWord = domainEntries.get(i - 1).get(words.getUnchecked(i - 1));

            assert domainEntryForPreviousWord != null;

            domainEntryForPreviousWord.insertConnectionToWord(words.getUnchecked(i));
        }

        countsDirty = true;
    }

    /**
     * Thread-safe assuming otherDomainStorage is not being concurrently
     * accessed by other threads and that there are no concurrent threads
     * affecting the same DomainEntries (i.e, each thread handles different wordIds).
     */
    @Override
    public void aggregate(DomainStorage otherDomainStorage) {
        int otherNumberOfDomains = otherDomainStorage.numberOfDomains;

        if (numberOfDomains == -1) {
            setNumberOfDomains(otherDomainStorage.getNumberOfDomains());
        }

        if (numberOfDomains != otherNumberOfDomains) {
            throw new RuntimeException("Different number of " +
                    "domains: " + numberOfDomains + " vs " + otherNumberOfDomains);
        }

        for (int i = 0; i < numberOfDomains; ++i) {
            Map<Integer, DomainEntry> thisDomainMap = domainEntries.get(i);
            Map<Integer, DomainEntry> otherDomainMap = otherDomainStorage.domainEntries.get(i);

            for (Map.Entry<Integer, DomainEntry> otherDomainMapEntry : otherDomainMap.entrySet()) {
                Integer otherVertexId = otherDomainMapEntry.getKey();
                DomainEntry otherDomainEntry = otherDomainMapEntry.getValue();

                DomainEntry thisDomainEntry = thisDomainMap.get(otherVertexId);

                if (thisDomainEntry == null) {
                    thisDomainMap.put(otherVertexId, otherDomainEntry);
                } else {
                    thisDomainEntry.aggregate(otherDomainEntry);
                }
            }
        }

        countsDirty = true;
    }

    @Override
    public long getNumberOfEnumerations() {
        if (countsDirty) {
            throw new RuntimeException("Should have never been the case");
        }

        long num = 0;

        if (domainEntries.size() <= 0) {
            return num;
        }

        for (DomainEntry domainEntry : domainEntries.get(0).values()) {
            num += domainEntry.getCounter();
        }

        return num;
    }

    @Override
    public void clear() {
        if (domainEntries != null) {
            for (Map<Integer, DomainEntry> domainMap : domainEntries) {
                domainMap.clear();
            }
        }

        domain0OrderedKeys = null;
        countsDirty = false;
    }

    @Override
    public StorageReader getReader(Pattern pattern,
            Computation<Embedding> computation,
            int numPartitions, int numBlocks, int maxBlockSize) {
        throw new RuntimeException("Shouldn't be read");
    }

    public void finalizeConstruction(ExecutorService pool, int numParts) {
        recalculateCounts(pool, numParts);
        orderDomain0Keys();
    }

    private void orderDomain0Keys() {
        domain0OrderedKeys = Ints.toArray(domainEntries.get(0).keySet());
        Arrays.sort(domain0OrderedKeys);
    }

    private class RecalculateTask implements Runnable {
        private int partId;
        private int totalParts;
        private int domain;

        public RecalculateTask(int partId, int totalParts) {
            this.partId = partId;
            this.totalParts = totalParts;
        }

        public void setDomain(int domain) {
            this.domain = domain;
        }

        @Override
        public void run() {
            Map<Integer, DomainEntry> currentEntryMap = domainEntries.get(domain);
            Map<Integer, DomainEntry> followingEntryMap = domainEntries.get(domain + 1);

            for (Map.Entry<Integer, DomainEntry> entry : currentEntryMap.entrySet()) {
                int wordId = entry.getKey();

                if (wordId % totalParts == partId) {
                    DomainEntry domainEntry = entry.getValue();
                    domainEntry.setCounter(0);
                    domainEntry.incrementCounterFrom(followingEntryMap);
                }
            }
        }
    }

    private void recalculateCounts(ExecutorService pool, int numParts) {
        if (!countsDirty || numberOfDomains == 0) {
            return;
        }

        // All entries in the last domain necessarily have a count of 1 since they have no connections.
        for (DomainEntry domainEntry : domainEntries.get(numberOfDomains - 1).values()) {
            domainEntry.setCounter(1);
        }

        RecalculateTask[] tasks = new RecalculateTask[numParts];

        for (int i = 0; i < tasks.length; ++i) {
            tasks[i] = new RecalculateTask(i, numParts);
        }

        Future[] futures = new Future[numParts];

        for (int i = numberOfDomains - 2; i >= 0; --i) {
            for (int j = 0; j < numParts; ++j) {
                RecalculateTask task = tasks[j];
                task.setDomain(i);
                futures[j] = pool.submit(task);
            }

            for (Future future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        countsDirty = false;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(numberOfDomains);

        for (Map<Integer, DomainEntry> domainEntryMap : domainEntries) {
            dataOutput.writeInt(domainEntryMap.size());
            for (Map.Entry<Integer, DomainEntry> entry : domainEntryMap.entrySet()) {
                Integer wordId = entry.getKey();
                DomainEntry domainEntry = entry.getValue();
                dataOutput.writeInt(wordId);
                domainEntry.write(dataOutput, writerSetConsumer);
            }
        }
    }

    public void write(DataOutput[] outputs, boolean[] hasContent) throws IOException {
        int numParts = outputs.length;
        int[] numEntriesOfPartsInDomain = new int[numParts];

        for (int i = 0; i < numParts; ++i) {
            outputs[i].writeInt(numberOfDomains);
        }

        for (Map<Integer, DomainEntry> domainEntryMap : domainEntries) {
            Arrays.fill(numEntriesOfPartsInDomain, 0);

            for (Integer wordId : domainEntryMap.keySet()) {
                int partId = wordId % numParts;

                ++numEntriesOfPartsInDomain[partId];
            }

            for (int i = 0; i < numParts; ++i) {
                int numEntriesOfPartInDomain = numEntriesOfPartsInDomain[i];
                outputs[i].writeInt(numEntriesOfPartInDomain);

                if (numEntriesOfPartInDomain > 0) {
                    hasContent[i] = true;
                }
            }

            Iterator<Map.Entry<Integer, DomainEntry>> domainEntryMapIterator
                    = domainEntryMap.entrySet().iterator();

            while (domainEntryMapIterator.hasNext()) {
                Map.Entry<Integer, DomainEntry> entry = domainEntryMapIterator.next();
                int wordId = entry.getKey();
                int partId = wordId % numParts;

                DataOutput output = outputs[partId];

                output.writeInt(wordId);

                DomainEntry domainEntry = entry.getValue();
                domainEntry.write(output, writerSetConsumer);

                domainEntryMapIterator.remove();
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.clear();

        setNumberOfDomains(dataInput.readInt());
        for (int i = 0; i < numberOfDomains; ++i) {
            int domainEntryMapSize = dataInput.readInt();
            Map<Integer, DomainEntry> domainEntryMap = domainEntries.get(i);

            for (int j = 0; j < domainEntryMapSize; ++j) {
                int wordId = dataInput.readInt();
                DomainEntrySet domainEntry = new DomainEntrySet();
                domainEntry.readFields(dataInput);
                domainEntryMap.put(wordId, domainEntry);
            }
        }

        countsDirty = true;
    }

    @Override
    public String toString() {
        return toStringResume();
    }

    public int getNumberOfDomains() {
        return numberOfDomains;
    }

    public StorageStats getStats() {
        StorageStats stats = new StorageStats();

        stats.numDomains = domainEntries.size();

        for (Map<Integer, DomainEntry> domainMap : domainEntries) {
            int domainSize = domainMap.size();

            if (domainSize > stats.maxDomainSize) {
                stats.maxDomainSize = domainSize;
            }

            if (domainSize < stats.minDomainSize) {
                stats.minDomainSize = domainSize;
            }

            stats.sumDomainSize += domainSize;

            for (DomainEntry domainEntry : domainMap.values()) {
                int numPointers = domainEntry.getNumPointers();
                int numWastedPointers = domainEntry.getWastedPointers();

                if (numPointers > stats.maxPointersSize) {
                    stats.maxPointersSize = numPointers;
                }

                if (numPointers < stats.minPointersSize) {
                    stats.minPointersSize = numPointers;
                }

                stats.sumPointersSize += numPointers;
                stats.sumWastedPointers += numWastedPointers;
            }
        }

        return stats;
    }

    public String toStringResume() {
        StringBuilder sb = new StringBuilder();
        sb.append("DomainStorage{");
        sb.append("enumerations=");
        sb.append(getNumberOfEnumerations());
        sb.append(", ");

        for (int i = 0; i < domainEntries.size(); i++) {
            sb.append("Domain[" + i + "] size " + domainEntries.get(i).size() + "\n");
        }

        return sb.toString();
    }

    public String toStringDebug() {
        StringBuilder sb = new StringBuilder();

        sb.append("@EmbeddingsZip\n");
        sb.append("Total Enumerations: " + getNumberOfEnumerations() + "\n");
        for (int i = 0; i < domainEntries.size(); i++) {
            sb.append("Domain[" + i + "] size " + domainEntries.get(i).size() + "\n");
            TreeSet<Integer> orderedIds = new TreeSet<>();
            Map<Integer, TreeSet<Integer>> connections = new TreeMap<>();

            long counterSum = 0;

            for (Map.Entry<Integer, DomainEntry> entry : domainEntries.get(i).entrySet()) {
                Integer wordId = entry.getKey();
                DomainEntry domainEntry = entry.getValue();

                counterSum += domainEntry.getCounter();

                orderedIds.add(wordId);

                TreeSet<Integer> neighbours = new TreeSet<>();
                connections.put(wordId, neighbours);

                //This debug function will fail if called on the readOnly domain.
                IntCursor neighbourCursor = domainEntry.getPointersCursor();

                while (neighbourCursor.moveNext()) {
                    neighbours.add(neighbourCursor.elem());
                }
            }

            sb.append("counterSum=");
            sb.append(counterSum);
            sb.append('\n');

            sb.append(orderedIds);
            sb.append('\n');

            for (Map.Entry<Integer, TreeSet<Integer>> mapEntry : connections.entrySet()) {
                sb.append(mapEntry.getKey());
                sb.append('=');
                sb.append(mapEntry.getValue());
                sb.append('\n');
            }
        }

        return sb.toString();
    }
}
