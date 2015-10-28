package io.arabesque.pattern;

import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

import java.util.Arrays;

public class VertexPositionEquivalences {
    private IntSet[] equivalences;
    private int numVertices;

    public VertexPositionEquivalences() {
        this.equivalences = null;
    }

    public void setNumVertices(int numVertices) {
        if (this.numVertices != numVertices) {
            ensureCapacity(numVertices);
            this.numVertices = numVertices;
        }
    }

    public void clear() {
        for (int i = 0; i < equivalences.length; ++i) {
            equivalences[i].clear();
        }

        for (int i = 0; i < numVertices; ++i) {
            equivalences[i].add(i);
        }
    }

    public void addEquivalence(int pos1, int pos2) {
        equivalences[pos1].add(pos2);
    }

    public IntSet getEquivalences(int pos) {
        return equivalences[pos];
    }

    public void propagateEquivalences() {
        for (int i = 0; i < numVertices; ++i) {
            IntSet currentVertexEquivalences = equivalences[i];

            if (currentVertexEquivalences != null) {
                IntCursor cursor = currentVertexEquivalences.cursor();

                while (cursor.moveNext()) {
                    int equivalentPosition = cursor.elem();

                    if (equivalentPosition == i) {
                        continue;
                    }

                    equivalences[i].addAll(equivalences[equivalentPosition]);
                }
            }
        }
    }

    private void ensureCapacity(int n) {
        int numSetsToCreate = n;

        if (equivalences == null) {
            equivalences = new HashIntSet[n];
        }
        else if (equivalences.length < n) {
            numSetsToCreate -= equivalences.length;
            equivalences = Arrays.copyOf(equivalences, n);
        }

        int newSize = equivalences.length;
        int targetI = newSize - numSetsToCreate;

        for (int i = newSize - 1; i >= targetI; --i) {
            equivalences[i] = HashIntSets.newMutableSet();
        }
    }

    public int getNumVertices() {
        return numVertices;
    }

    @Override
    public String toString() {
        return "VertexPositionEquivalences{" +
                "equivalences=" + Arrays.toString(equivalences) +
                ", numVertices=" + numVertices +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VertexPositionEquivalences that = (VertexPositionEquivalences) o;

        if (numVertices != that.numVertices) return false;

        for (int i = 0; i < numVertices; ++i) {
            // Only enter if one is null and the other isn't
            if ((equivalences[i] == null) ^ (that.equivalences[i] == null)) {
                return false;
            }

            if (equivalences[i] != null && !equivalences[i].equals(that.equivalences[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = numVertices;

        for (int i = 0; i < numVertices; ++i) {
            result = 31 * result + (equivalences[i] != null ? equivalences[i].hashCode() : 0);
        }

        return result;
    }
}
