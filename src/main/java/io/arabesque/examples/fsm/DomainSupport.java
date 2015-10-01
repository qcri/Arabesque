package io.arabesque.examples.fsm;

import io.arabesque.aggregation.PatternAggregationAwareValue;
import io.arabesque.embedding.Embedding;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.ClearSetConsumer;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class DomainSupport implements Writable, PatternAggregationAwareValue {
    private static final ThreadLocal<ClearSetConsumer> clearSetConsumer =
            new ThreadLocal<ClearSetConsumer>() {
                @Override
                protected ClearSetConsumer initialValue() {
                    return new ClearSetConsumer();
                }
            };

    private HashIntSet[] domainSets;
    private HashIntSet domainsReachedSupport;
    private boolean enoughSupport;
    private int support;
    private int numberOfDomains;

    public DomainSupport() {
        this.numberOfDomains = 0;
        this.domainsReachedSupport = HashIntSets.newMutableSet();
        this.enoughSupport = false;
    }

    public DomainSupport(int support) {
        this();
        this.support = support;
    }

    public void setFromEmbedding(Embedding embedding) {
        numberOfDomains = embedding.getNumVertices();
        ensureCanStoreNDomains(numberOfDomains);

        clear();

        int vertexMap[] = embedding.getVertices();

        for (int i = 0; i < numberOfDomains; i++) {
            if (hasDomainReachedSupport(i)) {
                continue;
            }

            HashIntSet domain = getDomainSet(i);

            domain.add(vertexMap[i]);

            if (domain.size() >= support) {
                insertDomainsAsFrequent(i);
            }
        }
    }

    public int getSupport() {
        return support;
    }

    private boolean hasDomainReachedSupport(int i) {
        return i < numberOfDomains && (enoughSupport || domainsReachedSupport.contains(i));
    }

    private void insertDomainsAsFrequent(int i) {
        if (enoughSupport) {
            return;
        }

        domainsReachedSupport.add(i);

        if (domainSets[i] != null) {
            domainSets[i].clear();
        }

        if (domainsReachedSupport.size() == numberOfDomains) {
            this.clear();
            enoughSupport = true;
        }
    }

    private HashIntSet getDomainSet(int i) {
        if (i >= numberOfDomains || i >= domainSets.length) return null;

        HashIntSet domainSet = domainSets[i];

        if (domainSet == null) {
            domainSet = domainSets[i] = HashIntSets.newMutableSet();
        }

        return domainSet;
    }

    public void ensureCanStoreNDomains(int size) {
        if (domainSets == null) {
            this.domainSets = new HashIntSet[size];
        } else if (domainSets.length < size) {
            domainSets = Arrays.copyOf(domainSets, size);
        }
    }

    public void clear() {
        if (domainSets != null) {
            for (HashIntSet domain : domainSets) {
                if (domain != null) {
                    domain.clear();
                }
            }
        }

        domainsReachedSupport.clear();

        enoughSupport = false;
    }

    public boolean hasEnoughSupport() {
        return enoughSupport;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(support);
        dataOutput.writeInt(numberOfDomains);

        if (enoughSupport) {
            dataOutput.writeBoolean(true);
        } else {
            dataOutput.writeBoolean(false);

            dataOutput.writeInt(domainsReachedSupport.size());
            for (int domain : domainsReachedSupport) {
                dataOutput.writeInt(domain);
            }

            for (int i = 0; i < numberOfDomains; ++i) {
                if (domainsReachedSupport.contains(i)) {
                    continue;
                }

                dataOutput.writeInt(domainSets[i].size());
                for (int id : domainSets[i]) {
                    dataOutput.writeInt(id);
                }
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.clear();

        support = dataInput.readInt();
        numberOfDomains = dataInput.readInt();

        if (dataInput.readBoolean()) {
            enoughSupport = true;
        } else {
            enoughSupport = false;

            ensureCanStoreNDomains(numberOfDomains);

            int numDomainsReachedSupport = dataInput.readInt();
            for (int i = 0; i < numDomainsReachedSupport; ++i) {
                domainsReachedSupport.add(dataInput.readInt());
            }

            for (int i = 0; i < numberOfDomains; ++i) {
                if (domainsReachedSupport.contains(i)) {
                    continue;
                }

                int domainSize = dataInput.readInt();

                HashIntSet domainSet = getDomainSet(i);

                for (int j = 0; j < domainSize; ++j) {
                    domainSet.add(dataInput.readInt());
                }
            }

        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                "domainsReachedSupport=" + domainsReachedSupport +
                ", enoughSupport=" + enoughSupport +
                ", support=" + support +
                ", numberOfDomains=" + numberOfDomains);

        if (domainSets != null) {
            for (int i = 0; i < numberOfDomains; i++) {
                HashIntSet domainSet = domainSets[i];

                if (domainSet == null) {
                    continue;
                }

                sb.append(",domain[" + i + "]=" + domainSets[i]);
            }
        }

        sb.append('}');

        return sb.toString();
    }

    public String toStringResume() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                "domainsReachedSupport=" + domainsReachedSupport +
                ", enoughSupport=" + enoughSupport +
                ", support=" + support +
                ", numberOfDomains=" + numberOfDomains);

        if (domainSets != null) {
            for (int i = 0; i < numberOfDomains; i++) {
                HashIntSet domainSet = domainSets[i];

                if (domainSet == null) {
                    continue;
                }

                sb.append(",domain[" + i + "]=" + domainSet.size());
            }
        }

        sb.append('}');

        return sb.toString();
    }

    public void aggregate(final HashIntSet[] domains) {
        if (enoughSupport)
            return;

        for (int i = 0; i < numberOfDomains; ++i) {
            aggregate(domains[i], i);

            if (enoughSupport) {
                break;
            }
        }
    }

    public void aggregate(HashIntSet otherDomain, int i) {
        if (otherDomain == null) {
            return;
        }

        if (hasDomainReachedSupport(i)) {
            return;
        }

        HashIntSet domain = getDomainSet(i);

        if (domain == otherDomain) {
            return;
        }

        domain.addAll(otherDomain);

        if (domain.size() >= support) {
            insertDomainsAsFrequent(i);
        }
    }

    public void aggregate(DomainSupport other) {
        if (this == other)
            return;

        // If we already have support, do nothing
        if (this.enoughSupport) {
            return;
        }

        // If the other has enough support, make us have enough support too
        if (other.enoughSupport) {
            this.clear();
            this.enoughSupport = true;
            return;
        }

        if (getNumberOfDomains() != other.getNumberOfDomains()) {
            throw new RuntimeException("Incompatible aggregation of DomainSupports: # of domains differs");
        }

        this.domainsReachedSupport.addAll(other.domainsReachedSupport);

        if (domainsReachedSupport.size() == numberOfDomains) {
            this.clear();
            this.enoughSupport = true;
            return;
        }

        ClearSetConsumer clearConsumer = clearSetConsumer.get();
        clearConsumer.setSupportMatrix(this.domainSets);
        other.domainsReachedSupport.forEach(clearConsumer);

        aggregate(other.domainSets);
    }

    public int getNumberOfDomains() {
        return numberOfDomains;
    }

    @Override
    public void handleConversionFromQuickToCanonical(Pattern quickPattern, Pattern canonicalPattern) {
        if (hasEnoughSupport()) {
            return;
        }

        // Taking into account automorphisms of the quick pattern, merge
        // equivalent positions
        HashIntSet domainEquivalences[] = quickPattern.getAutoVertexSet();

        if (domainEquivalences.length != numberOfDomains) {
            throw new RuntimeException("Mismatch between # number domains and size of autovertexset");
        }

        for (int i = 0; i < numberOfDomains; ++i) {
            IntCursor cursor = domainEquivalences[i].cursor();
            HashIntSet currentDomainSet = getDomainSet(i);

            while (cursor.moveNext()) {
                int equivalentDomainIndex = cursor.elem();

                if (hasDomainReachedSupport(i)) {
                    insertDomainsAsFrequent(equivalentDomainIndex);
                } else {
                    aggregate(currentDomainSet, equivalentDomainIndex);
                }
            }
        }

        // Rearrange to match canonical pattern structure
        HashIntIntMap canonicalLabeling = canonicalPattern.getCanonicalLabeling();

        HashIntSet[] oldDomainSets = Arrays.copyOf(domainSets, numberOfDomains);
        HashIntSet oldDomainsReachedSupport = HashIntSets.newMutableSet(domainsReachedSupport);
        domainsReachedSupport.clear();

        for (int i = 0; i < numberOfDomains; ++i) {
            // Equivalent position in the canonical pattern to position i in the quick pattern
            int minDomainIndex = canonicalLabeling.get(i);

            domainSets[minDomainIndex] = oldDomainSets[i];

            if (oldDomainsReachedSupport.contains(i)) {
                domainsReachedSupport.add(minDomainIndex);
            }
        }
    }
}
