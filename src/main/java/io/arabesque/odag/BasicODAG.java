package io.arabesque.odag;

import io.arabesque.conf.Configuration;
import io.arabesque.conf.SparkConfiguration;
import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.*;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.concurrent.ExecutorService;

public abstract class BasicODAG implements Writable, Externalizable {
    protected AbstractDomainStorage storage;
    protected boolean serializeAsReadOnly;

    protected AbstractDomainStorage createDomainStorage(boolean readOnly) {
        String commStrategy = Configuration.get().getCommStrategy();

        if (readOnly) {
            if (commStrategy.equals(SparkConfiguration.COMM_ODAG_SP()) || commStrategy.equals(SparkConfiguration.COMM_ODAG_SP_PRIM()))
                return new PrimitiveDomainStorageReadOnly();
            else
                return new GenericDomainStorageReadOnly();
        }
        else {
            if (commStrategy.equals(SparkConfiguration.COMM_ODAG_SP()) || commStrategy.equals(SparkConfiguration.COMM_ODAG_SP_PRIM()))
                return new PrimitiveDomainStorage();
            else
                return new GenericDomainStorage();
        }
    }

    public abstract void addEmbedding(Embedding embedding);
    public abstract Pattern getPattern();
    public abstract StorageReader getReader(
          Computation<Embedding> computation,
          int numPartitions,
          int numBlocks,
          int maxBlockSize);
    public abstract void aggregate(BasicODAG embZip);

    public int getNumberOfDomains() {
        return storage.getNumberOfDomains();
    }

    public AbstractDomainStorage getStorage() {
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
