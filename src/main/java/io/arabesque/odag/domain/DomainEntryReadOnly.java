package io.arabesque.odag.domain;

import io.arabesque.utils.WriterSetConsumer;
import net.openhft.koloboke.collect.IntCursor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class DomainEntryReadOnly extends DomainEntrySet {
    @Override
    public String toString() {
        return "DomainEntryReadOnly{" +
                "pointers=" + Arrays.toString(pointers) + super.getCounter() +
                '}';
    }

    private int[] pointers;

    @Override
    public boolean insertConnectionToWord(int p) {
        throw new RuntimeException("Not allowed");
    }

    @Override
    public void aggregate(DomainEntry otherDomainEntry) {
        throw new RuntimeException("Not allowed");
    }

    @Override
    public IntCursor getPointersCursor() {
        throw new RuntimeException("Use something more efficient");
    }

    @Override
    public void write(DataOutput dataOutput,
            WriterSetConsumer writerSetConsumer)
            throws IOException {
        throw new RuntimeException("It's read only, writing not allowed(?)");
    }

    public int getNumPointers() {
        return pointers != null ? pointers.length : 0;
    }

    public int[] getPointers() {
        return pointers;
    }

    /**
     * Reading it.
     *
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            int numPointers = dataInput.readInt();
            pointers = new int[numPointers];

            for (int i = 0; i < numPointers; ++i) {
                pointers[i] = dataInput.readInt();
            }
        } else {
            pointers = null;
        }
    }

    /**
     * We don't waste pointers in the read only domains.
     *
     * @return
     */
    public int getWastedPointers() {
        return 0;
    }

    @Override
    public void incrementCounterFrom(Map<Integer, DomainEntry> followingEntryMap) {
        for (int i = 0; i < pointers.length; i++) {
            DomainEntry domainEntryOfPointer = followingEntryMap.get(pointers[i]);
            assert domainEntryOfPointer != null;
            incrementCounter(domainEntryOfPointer.getCounter());

        }
    }
}