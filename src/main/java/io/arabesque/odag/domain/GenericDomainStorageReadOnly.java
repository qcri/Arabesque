package io.arabesque.odag.domain;

import com.koloboke.collect.IntCollection;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.LabelledEdge;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.LabelledPatternEdge;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.PatternEdge;
import io.arabesque.pattern.PatternEdgeArrayList;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

public class GenericDomainStorageReadOnly extends GenericDomainStorage {
    private static final Logger LOG = Logger.getLogger(GenericDomainStorageReadOnly.class);

    @Override
    public void readFields(DataInput dataInput) throws IOException { 
        this.clear();

        numEmbeddings = dataInput.readLong();
        setNumberOfDomains(dataInput.readInt());

        ArrayList<ConcurrentHashMap<Integer, DomainEntry>> domains = (ArrayList<ConcurrentHashMap<Integer, DomainEntry>>)domainEntries;

        for (int i = 0; i < numberOfDomains; ++i) {
            int domainEntryMapSize = dataInput.readInt();

            ConcurrentHashMap<Integer, DomainEntry> domainEntryMap = domains.get(i);

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
        return new GenericSinglePatternReader(pattern, computation, numPartitions, numBlocks, maxBlockSize);
    }

    @Override
    public StorageReader getReader(Pattern[] patterns,
            Computation<Embedding> computation,
            int numPartitions, int numBlocks, int maxBlockSize) {
        return new GenericMultiPatternReader(patterns, computation, numPartitions, numBlocks, maxBlockSize);
    }
}
