package io.arabesque.odag.domain;

import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import io.arabesque.embedding.Embedding;
import io.arabesque.utils.WriterSetConsumer;
import io.arabesque.utils.collection.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class PrimitiveDomainStorage extends AbstractDomainStorage<PrimitiveDomainStorage> {

    public PrimitiveDomainStorage(int numberOfDomains) {
        setNumberOfDomains(numberOfDomains);
        countsDirty = false;
        keysOrdered = false;
        writerSetConsumer = new WriterSetConsumer();
        numEmbeddings = 0;
    }

    public PrimitiveDomainStorage() {
        numberOfDomains = -1;
        countsDirty = false;
        keysOrdered = false;
        writerSetConsumer = new WriterSetConsumer();
        numEmbeddings = 0;
    }

    @Override
    public ArrayList<IntObjMap<DomainEntry>> getDomainEntries() {
       return (ArrayList<IntObjMap<DomainEntry>>) domainEntries;
    }

    @Override
    protected synchronized void ensureCanStoreNDomains(int nDomains) {
        if (nDomains < 0) {
            return;
        }

        if (domainEntries == null) {
            domainEntries = new ArrayList<IntObjMap<DomainEntry>>(nDomains);
        } else {
            domainEntries.ensureCapacity(nDomains);
        }

        int currentNumDomains = domainEntries.size();
        int delta = nDomains - currentNumDomains;

        for (int i = 0; i < delta; ++i) {
            IntObjMap<DomainEntry> newDomain = HashIntObjMaps.newMutableMap();
            domainEntries.add(newDomain);
        }

    }

    @Override
    public long getDomainEntriesCalculatedSizeInBytes() {
        long size = 0;
        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        for(int i = 0 ; i < domains.size() ; ++i) {
            IntObjMap domain = domains.get(i);

            // size to store keys (4 bytes for each primitive int)
            size += (4 * domain.size());

            // size to store values
            Object[] entries = domain.values().toArray();

            for(int j = 0 ; j < entries.length ; ++j) {
                Object entry = entries[j];
                if(entry instanceof DomainEntryReadOnly) {
                    size += ((DomainEntryReadOnly)entry).getNumPointers() * 4;
                }
                else {
                    size += ((DomainEntrySet)entry).getNumPointers() * 8;
                }
            }
        }

        return size;
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        int numWords = embedding.getNumWords();
        IntArrayList words = embedding.getWords();
        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        if (domains.size() != numWords) {
            throw new RuntimeException("Tried to add an embedding with wrong number " +
                    "of expected vertices (" + domains.size() + ") " + embedding);
        }

        for (int i = 0; i < numWords; ++i) {
            synchronized (domainEntries.get(i)) {
                DomainEntry domainEntryForCurrentWord = domains.get(i).get(words.getUnchecked(i));

                if (domainEntryForCurrentWord == null) {
                    domainEntryForCurrentWord = new DomainEntrySet();
                    domains.get(i).put(words.getUnchecked(i), domainEntryForCurrentWord);
                }
            }
        }

        for (int i = numWords - 1; i > 0; --i) {
            synchronized (domainEntries.get(i - 1)) {
                DomainEntry domainEntryForPreviousWord = domains.get(i - 1).get(words.getUnchecked(i - 1));

                assert domainEntryForPreviousWord != null;

                domainEntryForPreviousWord.insertConnectionToWord(words.getUnchecked(i));
            }
        }

        countsDirty = true;
        numEmbeddings++;
    }

    /**
     * Thread-safe assuming otherDomainStorage is not being concurrently
     * accessed by other threads and that there are no concurrent threads
     * affecting the same DomainEntries (i.e, each thread handles different wordIds).
     */
    @Override
    public void aggregate(PrimitiveDomainStorage otherDomainStorage) {
        int otherNumberOfDomains = otherDomainStorage.numberOfDomains;

        if (numberOfDomains == -1) {
            setNumberOfDomains(otherDomainStorage.getNumberOfDomains());
        }

        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        if (numberOfDomains != otherNumberOfDomains) {
            throw new RuntimeException("Different number of " +
                    "domains: " + numberOfDomains + " vs " + otherNumberOfDomains);
        }

        for (int i = 0; i < numberOfDomains; ++i) {
            IntObjMap<DomainEntry> thisDomainMap = domains.get(i);
            IntObjMap<DomainEntry> otherDomainMap = ((ArrayList<IntObjMap<DomainEntry>>)otherDomainStorage.domainEntries).get(i);

            for (Map.Entry<Integer, DomainEntry> otherDomainMapEntry : otherDomainMap.entrySet()) {
                int otherVertexId = otherDomainMapEntry.getKey();
                DomainEntry otherDomainEntry = otherDomainMapEntry.getValue();

                DomainEntry thisDomainEntry = thisDomainMap.get(otherVertexId);

                if (thisDomainEntry == null) {
                    thisDomainMap.put(otherVertexId, otherDomainEntry);
                } else {
                    thisDomainEntry.aggregate(otherDomainEntry);
                }
            }

            for (Map.Entry<Integer, DomainEntry> otherDomainMapEntry : otherDomainMap.entrySet()) {
                int otherVertexId = otherDomainMapEntry.getKey();
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
        numEmbeddings += otherDomainStorage.numEmbeddings;
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
            synchronized (this) {
                ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

                IntObjMap<DomainEntry> currentEntryMap = domains.get(domain);
                IntObjMap<DomainEntry> followingEntryMap = domains.get(domain + 1);

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
    }

    @Override
    protected void recalculateCounts(ExecutorService pool, int numParts) {
        if (!countsDirty || numberOfDomains == 0) {
            return;
        }

        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        // All entries in the last domain necessarily have a count of 1 since they have no connections.
        for (DomainEntry domainEntry : domains.get(numberOfDomains - 1).values()) {
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
        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        dataOutput.writeLong(numEmbeddings);
        dataOutput.writeInt(numberOfDomains);

        for (IntObjMap<DomainEntry> domainEntryMap : domains) {
            dataOutput.writeInt(domainEntryMap.size());
            for (Map.Entry<Integer, DomainEntry> entry : domainEntryMap.entrySet()) {
                Integer wordId = entry.getKey();
                DomainEntry domainEntry = entry.getValue();
                dataOutput.writeInt(wordId);
                domainEntry.write(dataOutput, writerSetConsumer);
            }
        }
    }

    @Override
    public void write(DataOutput[] outputs, boolean[] hasContent) throws IOException {
        int numParts = outputs.length;
        int[] numEntriesOfPartsInDomain = new int[numParts];
        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        for (int i = 0; i < numParts; ++i) {
            outputs[i].writeLong(numEmbeddings);
            outputs[i].writeInt(numberOfDomains);
        }

        for (IntObjMap<DomainEntry> domainEntryMap : domains) {
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

        numEmbeddings = dataInput.readLong();
        setNumberOfDomains(dataInput.readInt());

        ArrayList<IntObjMap<DomainEntry>> domains = (ArrayList<IntObjMap<DomainEntry>>)domainEntries;

        for (int i = 0; i < numberOfDomains; ++i) {
            int domainEntryMapSize = dataInput.readInt();
            IntObjMap<DomainEntry> domainEntryMap = domains.get(i);

            for (int j = 0; j < domainEntryMapSize; ++j) {
                int wordId = dataInput.readInt();
                DomainEntrySet domainEntry = new DomainEntrySet();
                domainEntry.readFields(dataInput);
                domainEntryMap.put(wordId, domainEntry);
            }
        }
        countsDirty = true;
    }
}
