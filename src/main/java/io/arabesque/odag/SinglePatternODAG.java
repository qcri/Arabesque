package io.arabesque.odag;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.DomainStorage;
import io.arabesque.odag.domain.DomainStorageReadOnly;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.concurrent.ExecutorService;

public class SinglePatternODAG extends BasicODAG<SinglePatternODAG> {
    private Pattern pattern;
    private boolean serializeAsReadOnly;

    public SinglePatternODAG(Pattern pattern, int numberOfDomains) {
        this.pattern = pattern;
        serializeAsReadOnly = false;
        storage = new DomainStorage(numberOfDomains);
    }

    public SinglePatternODAG(boolean readOnly) {
        storage = createDomainStorage(readOnly);
    }

    public SinglePatternODAG() {
    }

    private DomainStorage createDomainStorage(boolean readOnly) {
        if (readOnly) return new DomainStorageReadOnly();
        else return new DomainStorage();
    }

    @Override
    public void addEmbedding(Embedding embedding) {
        if (pattern == null) {
            throw new RuntimeException("Tried to add an embedding without letting embedding zip know about the pattern");
        }

        storage.addEmbedding(embedding);
    }

    @Override
    public void aggregate(SinglePatternODAG embZip) {
        if (embZip == null) return;

        storage.aggregate(embZip.storage);
    }

    @Override
    public StorageReader getReader(Computation<Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
        return storage.getReader(pattern, computation, numPartitions, numBlocks, maxBlockSize);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        storage.write(out);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
       out.writeBoolean(serializeAsReadOnly);
       write(out);
    }

    public void writeInParts(DataOutput[] outputs, boolean[] hasContent) throws IOException {
        storage.write(outputs, hasContent);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.clear();
        storage.readFields(in);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
       serializeAsReadOnly = in.readBoolean();
       storage = createDomainStorage(serializeAsReadOnly);
       readFields(in);
    }

    public boolean getSerializeasWriteOnly() {
       return serializeAsReadOnly;
    }

    public void setSerializeAsReadOnly (boolean serializeAsReadOnly) {
       this.serializeAsReadOnly = serializeAsReadOnly;
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public String toString() {
       return "SinglePatternODAG(" +
          (pattern != null ? pattern.toString() : "") + ")" +
          storage.toString();
    }
}
