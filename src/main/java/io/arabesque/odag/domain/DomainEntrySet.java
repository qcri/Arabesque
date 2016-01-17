package io.arabesque.odag.domain;

import io.arabesque.utils.WriterSetConsumer;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

public class DomainEntrySet implements DomainEntry {
    private HashIntSet pointers;
    private long counter;

    public DomainEntrySet() {
        counter = 0;
    }

    public long getCounter() {
        return counter;
    }

    public void incrementCounter() {
        incrementCounter(1L);
    }

    public void incrementCounter(long inc) {
        counter += inc;
    }

    public boolean insertConnectionToWord(int p) {
        if (pointers == null) {
            pointers = HashIntSets.newMutableSet(8);
        }
        return pointers.add(p);
    }

    int size() {
        if (pointers == null) {
            return 0;
        }

        return pointers.size();
    }

    public IntCursor getPointersCursor() {
        if (pointers == null) {
            return null;
        }

        return pointers.cursor();
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public void aggregate(DomainEntry otherDomainEntry) {
        IntCursor otherPointersCursor = otherDomainEntry.getPointersCursor();

        if (otherPointersCursor == null) {
            return;
        }

        while (otherPointersCursor.moveNext()) {
            int pointedWordId = otherPointersCursor.elem();
            insertConnectionToWord(pointedWordId);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            int numPointers = dataInput.readInt();

            pointers = HashIntSets.newMutableSet(numPointers);

            for (int i = 0; i < numPointers; ++i) {
                pointers.add(dataInput.readInt());
            }
        } else {
            pointers = null;
        }
    }

    public void write(DataOutput dataOutput,
            WriterSetConsumer writerSetConsumer) throws IOException {
        if (pointers == null) {
            dataOutput.writeBoolean(false);
        } else {
            dataOutput.writeBoolean(true);
            dataOutput.writeInt(pointers.size());
            writerSetConsumer.setOutput(dataOutput);
            pointers.forEach(writerSetConsumer);
        }
    }

    public int getNumPointers() {
        return pointers != null ? pointers.size() : 0;
    }

    public int getWastedPointers() {
        if (pointers == null) {
            return 0;
        }

        try {
            Integer maxSize = getFieldValueFromInheritanceHierarchy(pointers, "maxSize");

            return maxSize - pointers.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void incrementCounterFrom(Map<Integer, DomainEntry> followingEntryMap) {
        IntCursor pointersCursor = getPointersCursor();
        while (pointersCursor.moveNext()) {
            DomainEntry domainEntryOfPointer = followingEntryMap.get(pointersCursor.elem());
            assert domainEntryOfPointer != null;
            incrementCounter(domainEntryOfPointer.getCounter());
        }
    }

    <V> V getFieldValueFromInheritanceHierarchy(Object object, String fieldName) throws IllegalAccessException {
        Class<?> i = object.getClass();

        while (i != null && i != Object.class) {
            for (Field field : i.getDeclaredFields()) {
                if (field.getName().equals(fieldName)) {
                    field.setAccessible(true);
                    return (V) field.get(object);
                }
            }
            i = i.getSuperclass();
        }

        return null;
    }
}
