package io.arabesque.odag.domain;

import com.koloboke.collect.map.IntObjMap;
import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;

public class PrimitiveDomainStorageReadOnly extends PrimitiveDomainStorage {
    private static final Logger LOG = Logger.getLogger(PrimitiveDomainStorageReadOnly.class);

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
        return new PrimitiveSinglePatternReader(pattern, computation, numPartitions, numBlocks, maxBlockSize);
    }

    @Override
    public StorageReader getReader(Pattern[] patterns,
                                   Computation<Embedding> computation,
                                   int numPartitions, int numBlocks, int maxBlockSize) {
        return new PrimitiveMultiPatternReader(patterns, computation, numPartitions, numBlocks, maxBlockSize);
    }
}
