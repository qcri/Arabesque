package io.arabesque.gmlib.disconnectedGraphFSM;

import io.arabesque.aggregation.PatternAggregationAwareValue;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.VertexPositionEquivalences;
import io.arabesque.utils.ClearSetConsumer;
import io.arabesque.utils.IntWriterConsumer;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Arrays;

/*
Counting the support for a pattern based on the embedding occurrences across subgraphs in a disconnected graph
 */

public class DisconnectedGraphSupport implements Writable, Externalizable, PatternAggregationAwareValue {

    private HashIntSet subgraphsSupport;
    private boolean enoughSupport;
    private int support;
    private IntWriterConsumer intWriterConsumer;
    private IntCollectionAddConsumer intAdderConsumer;
    private boolean setFromEmbedding;
    private Embedding embedding;
    private int patternSize;

    public DisconnectedGraphSupport() {
        this.subgraphsSupport = HashIntSets.newMutableSet();
        this.enoughSupport = false;
        this.setFromEmbedding = false;
        this.patternSize = 0;
    }

    public DisconnectedGraphSupport(int support) {
        this();
        this.support = support;
    }

    public void setFromEmbedding(Embedding embedding) {
        setFromEmbedding = true;
        this.embedding = embedding;
    }

    public int getSupport() {
        return support;
    }

    public void setSupport(int support) {
       if (this.support != support) {
         this.support = support;
         this.clear();
       }
    }

    public void clear() {
        this.clearSubgraphsSupport();

        setFromEmbedding = false;
        embedding = null;
    }

    private int getCurrentSupport() {
        return subgraphsSupport.size();
    }

    private void clearSubgraphsSupport() {
        this.subgraphsSupport.clear();
        this.enoughSupport = false;
    }

    public boolean hasEnoughSupport() {
        return enoughSupport;
    }

    private int getVertexSubgraph(int vertexId) {
        MainGraph graph = Configuration.get().getMainGraph();

        int subgraph = graph.getVertex(vertexId).getSubgraphId();

        return subgraph;
    }

    private boolean checkBelongToSameSubgraph(Embedding embedding) {
        IntArrayList vertexMap = embedding.getVertices();

        int subgraph = getVertexSubgraph(vertexMap.getUnchecked(0));

        for (int i = 1; i < patternSize; i++) {
            int vertexId = vertexMap.getUnchecked(i);
            int vertexSubgraph = getVertexSubgraph(vertexId);

            if(subgraph != vertexSubgraph) {
                throw new RuntimeException("Incorrect Embedding: Vertices does not belong to the same subgraph. \nEmbedding: " + embedding.toOutputString() +
                "\nvertexId: " + vertexId + " subgraphId: " + vertexSubgraph);
            }
        }

        return true;
    }

    private void convertFromEmbeddingToNormal() {
        patternSize = embedding.getNumVertices();

        //this.clearSubgraphsSupport();

        IntArrayList vertexMap = embedding.getVertices();

        MainGraph graph = Configuration.get().getMainGraph();

        int subgraph = graph.getVertex(vertexMap.getUnchecked(0)).getSubgraphId();

        if(checkBelongToSameSubgraph(embedding)) {

            this.addSubgraphToSupport(subgraph);

            setFromEmbedding = false;
            embedding = null;
        }
    }
    
    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write (objOutput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (setFromEmbedding) {
            convertFromEmbeddingToNormal();
        }

        dataOutput.writeInt(support);
        dataOutput.writeInt(patternSize);

        if (enoughSupport) {
            dataOutput.writeBoolean(true);
        } else {
            dataOutput.writeBoolean(false);

            if (intWriterConsumer == null) {
                intWriterConsumer = new IntWriterConsumer();
            }

            intWriterConsumer.setDataOutput(dataOutput);

            dataOutput.writeInt(this.subgraphsSupport.size());
            this.subgraphsSupport.forEach(intWriterConsumer);
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields (objInput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.clear();

        support = dataInput.readInt();
        patternSize = dataInput.readInt();

        if (dataInput.readBoolean()) {
            enoughSupport = true;
        } else {
            enoughSupport = false;

            int numSubgraphs = dataInput.readInt();
            for (int i = 0; i < numSubgraphs; ++i) {
                this.subgraphsSupport.add(dataInput.readInt());
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                "subgraphsSupport=" + subgraphsSupport +
                ", enoughSupport=" + enoughSupport +
                ", support=" + support +
                ", patternSize=" + patternSize +
                ", currentSupport=" + getCurrentSupport() +
                ", intWriterConsumer=" + intWriterConsumer +
                ", intAdderConsumer=" + intAdderConsumer +
                ", setFromEmbedding=" + setFromEmbedding +
                ", embedding=" + embedding +
                "}");

        return sb.toString();
    }

    public String toStringResume() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                "subgraphsSupport=" + subgraphsSupport +
                ", enoughSupport=" + enoughSupport +
                ", support=" + support +
                ", patternSize=" + patternSize +
                "}");

        return sb.toString();
    }

    public void aggregate(final HashIntSet otherSubgraphsSupport) {
        if (enoughSupport)
            return;

        addAll(subgraphsSupport, otherSubgraphsSupport);

        if(subgraphsSupport.size() >= support) {
            this.clear();
            this.enoughSupport = true;
        }
    }

    private void addAll(IntCollection destination, IntCollection source) {
        if (intAdderConsumer == null) {
            intAdderConsumer = new IntCollectionAddConsumer();
        }

        intAdderConsumer.setCollection(destination);

        source.forEach(intAdderConsumer);
    }

    private void addSubgraphToSupport(int subgraph) {
        this.subgraphsSupport.add(subgraph);

        if (this.subgraphsSupport.size() >= this.support) {
            this.clear();
            this.enoughSupport = true;
        }
    }

    private void embeddingAggregate(Embedding embedding) {
        if(hasEnoughSupport())
            return;

        if (checkBelongToSameSubgraph(embedding)) {
            IntArrayList vertexMap = embedding.getVertices();

            MainGraph graph = Configuration.get().getMainGraph();

            int subgraph = graph.getVertex(vertexMap.getUnchecked(0)).getSubgraphId();

            this.addSubgraphToSupport(subgraph);
        }
    }

    public void aggregate(DisconnectedGraphSupport other) {
        if (this == other)
            return;

        // If we already have support, do nothing
        if (this.enoughSupport) {
            return;
        }

        // If other simply references an embedding, do special quick aggregate
        if (other.setFromEmbedding) {
            embeddingAggregate(other.embedding);
            return;
        }

        // If the other has enough support, make us have enough support too
        if (other.enoughSupport) {
            this.clear();
            this.enoughSupport = true;
            return;
        }

        addAll(subgraphsSupport, other.subgraphsSupport);

        if (subgraphsSupport.size() >= support) {
            this.clear();
            this.enoughSupport = true;
        }
    }

    public int getPatternSize() {
        return patternSize;
    }

    @Override
    public void handleConversionFromQuickToCanonical(Pattern quickPattern, Pattern canonicalPattern) {
        if (hasEnoughSupport()) {
            return;
        }

        // Taking into account automorphisms of the quick pattern, merge
        // equivalent positions
        //No need to do anything here to handle the automorphism since counting is based on the number of subgraphs
        //contributing to the frequent patterns. There is no domains to merge.
    }
}
