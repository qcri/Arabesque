package io.arabesque.compression;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public abstract class SimpleStorage implements Writable, Externalizable {
    protected UPSDomainStorage storage;
    protected boolean serializeAsReadOnly;

    protected UPSDomainStorage createDomainStorage(boolean readOnly) {
        if (readOnly) return new UPSDomainStorageReadOnly();
        else return new UPSDomainStorage();
    }

    public abstract void addEmbedding(Embedding embedding);
    public abstract Pattern getPattern();
    public abstract StorageReader getReader(
          Computation<Embedding> computation,
          int numPartitions,
          int numBlocks,
          int maxBlockSize);
    public abstract void aggregate(SimpleStorage embZip);

    public int getNumberOfDomains() {
        return storage.getNumberOfDomains();
    }

    public UPSDomainStorage getStorage() {
        return storage;
    }

    public long getNumberOfEnumerations() {
        return storage.getNumberOfEnumerations();
    }

    public void finalizeConstruction(ExecutorService pool, int numParts) {
        storage.finalizeConstruction(pool, numParts);
    }

    public void clear() {
        storage.clear();
    }

    public StorageStats getStats() {
        return storage.getStats();
    }

    public boolean getSerializeasWriteOnly() {
        return serializeAsReadOnly;
    }

    public void setSerializeAsReadOnly (boolean serializeAsReadOnly) {
        this.serializeAsReadOnly = serializeAsReadOnly;
    }

    public void writeInParts(DataOutput[] outputs, boolean[] hasContent) throws IOException {
        storage.write(outputs, hasContent);
    }

}
