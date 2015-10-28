package io.arabesque.utils;

import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.IntIterator;
import net.openhft.koloboke.function.IntConsumer;
import net.openhft.koloboke.function.IntPredicate;
import org.apache.hadoop.io.Writable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;

public class IntArrayList implements IntCollection, Writable {
    private static final int INITIAL_SIZE = 16;

    private int[] backingArray;
    private int numElements;

    public IntArrayList() {
        this(16);
    }

    public IntArrayList(int capacity) {
        ensureCapacity(capacity);
        this.numElements = 0;
    }

    public IntArrayList(Collection<Integer> collection) {
        this.ensureCapacity(collection.size());
        addAll(collection);
    }

    public IntArrayList(IntArrayList intArrayList) {
        numElements = intArrayList.numElements;
        backingArray = Arrays.copyOf(intArrayList.backingArray, numElements);
    }

    public int getSize() {
        return numElements;
    }

    public int getCapacity() {
        return backingArray.length;
    }

    public int getRemaining() {
        return getCapacity() - getSize();
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public long sizeAsLong() {
        return numElements;
    }

    @Override
    public boolean ensureCapacity(long l) {
        if (l > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("IntArrayList does not support long sizes yet");
        }

        int newTargetSize = (int) l;

        if (backingArray == null) {
            backingArray = new int[newTargetSize];
        }
        else if (newTargetSize > backingArray.length) {
            backingArray = Arrays.copyOf(backingArray, newTargetSize);
        }
        else {
            return false;
        }

        return true;
    }

    @Override
    public boolean shrink() {
        if (backingArray.length == numElements) {
            return false;
        }

        backingArray = Arrays.copyOf(backingArray, numElements);
        return true;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return contains((int) o);
    }

    @Override
    public boolean contains(int element) {
        for (int i = 0; i < numElements; ++i) {
            if (backingArray[i] == element) {
                return true;
            }
        }

        return false;
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        return toArray(new Integer[numElements]);
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts) {
        if (ts.length < numElements) {
            ts = Arrays.copyOf(ts, numElements);
        }

        Class<? extends T> classT = (Class<? extends T>) ts[0].getClass();

        for (int i = 0; i < numElements; ++i) {
            ts[i] = classT.cast(backingArray[i]);
        }

        if (ts.length > numElements) {
            ts[numElements] = null;
        }

        return ts;
    }

    @Nonnull
    @Override
    public int[] toIntArray() {
        return toArray(new int[numElements]);
    }

    @Nonnull
    @Override
    public int[] toArray(@Nonnull int[] ints) {
        if (ints.length < numElements) {
            return Arrays.copyOf(backingArray, numElements);
        }

        System.arraycopy(backingArray, 0, ints, 0, numElements);

        return ints;
    }

    /**
     * Removes all elements from the collection that are smaller than the provided value.
     * @param value Reference value.
     *
     * WARNING: This assumes the array is ordered (sort was called just before).
     */
    public void removeSmaller(int value) {
        int targetPosition = Arrays.binarySearch(backingArray, 0, numElements, value);

        if (targetPosition < 0) {
            targetPosition = -targetPosition - 1;
        }

        numElements -= targetPosition;

        if (targetPosition != 0 && numElements > 0) {
            System.arraycopy(backingArray, targetPosition, backingArray, 0, numElements);
        }
    }

    /**
     * Removes all elements from the collection that are bigger than the provided value.
     * @param value Reference value.
     *
     * WARNING: This assumes the array is ordered (sort was called just before).
     */
    public void removeBigger(int value) {
        int targetPosition = Arrays.binarySearch(backingArray, 0, numElements, value);

        if (targetPosition < 0) {
            targetPosition = -targetPosition - 1;
        }

        numElements = targetPosition;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(numElements);

        for (int i = 0; i < numElements; ++i) {
            dataOutput.writeInt(backingArray[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clear();

        numElements = dataInput.readInt();

        ensureCanAddNElements(numElements);

        for (int i = 0; i < numElements; ++i) {
            backingArray[i] = dataInput.readInt();
        }
    }

    private class IntArrayListCursor implements IntCursor {
        private int index;

        public IntArrayListCursor() {
            this.index = -1;
        }

        @Override
        public void forEachForward(@Nonnull IntConsumer intConsumer) {
            int localNumElements = numElements;

            for (int i = index; i < localNumElements; ++i) {
                intConsumer.accept(backingArray[i]);
            }

            if(localNumElements != numElements) {
                throw new ConcurrentModificationException();
            } else {
                this.index = numElements;
            }
        }

        @Override
        public int elem() {
            if (index < 0 || index >= numElements) {
                throw new IllegalStateException();
            }

            return backingArray[index];
        }

        @Override
        public boolean moveNext() {
            ++index;

            return index >= 0 && index < numElements;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class IntArrayListIterator implements IntIterator {
        private int index;

        public IntArrayListIterator() {
            this.index = -1;
        }

        @Override
        public int nextInt() {
            return backingArray[++index];
        }

        @Override
        public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
            int localNumElements = numElements;

            for (int i = index + 1; i < localNumElements - 1; ++i) {
                intConsumer.accept(backingArray[i]);
            }

            if (localNumElements != numElements) {
                throw new ConcurrentModificationException();
            } else {
                index = numElements - 1;
            }
        }

        @Override
        public boolean hasNext() {
            return index < numElements - 1;
        }

        @Override
        public Integer next() {
            return nextInt();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Nonnull
    @Override
    public IntCursor cursor() {
        return new IntArrayListCursor();
    }

    @Nonnull
    @Override
    public IntIterator iterator() {
        return new IntArrayListIterator();
    }

    @Override
    public void forEach(@Nonnull IntConsumer intConsumer) {
        for (int i = 0; i < numElements; ++i) {
            intConsumer.accept(backingArray[i]);
        }
    }

    @Override
    public boolean forEachWhile(@Nonnull IntPredicate intPredicate) {
        for (int i = 0; i < numElements; ++i) {
            if (!intPredicate.test(backingArray[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean add(@Nonnull Integer integer) {
        return add((int) integer);
    }

    public boolean add(int newValue) {
        ensureCanAddNewElement();
        backingArray[numElements++] = newValue;
        return true;
    }


    @Override
    public boolean remove(Object o) {
        return removeInt((int) o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Integer> c) {
        ensureCanAddNElements(c.size());

        for (int o : c) {
            add(o);
        }

        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return removeBasedOnCollection(c, true);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return removeBasedOnCollection(c, false);
    }

    private boolean removeBasedOnCollection(Collection<?> c, boolean ifPresent) {
        boolean removedAtLeastOne = false;

        for (int i = 0; i < numElements; ++i) {
            int e = backingArray[i];

            boolean collectionContainsE = c.contains(e);

            if (!(ifPresent ^ collectionContainsE)) {
                remove(i);
                removedAtLeastOne = true;
            }
        }

        return removedAtLeastOne;
    }

    @Override
    public boolean removeInt(int targetValue) {
        for (int i = 0; i < numElements; ++i) {
            int e = backingArray[i];

            if (e == targetValue) {
                remove(i);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean removeIf(@Nonnull IntPredicate intPredicate) {
        boolean removedAtLeastOne = false;

        for (int i = 0; i < numElements; ++i) {
            if (intPredicate.test(backingArray[i])) {
                removedAtLeastOne = true;
                remove(i);
            }
        }

        return removedAtLeastOne;
    }

    public int remove(int index) {
        if (index < 0 || index >= numElements) {
            throw new IllegalArgumentException();
        }

        int removedElement = backingArray[index];

        --numElements;

        if (index != numElements) {
            System.arraycopy(backingArray, index + 1, backingArray, index, numElements - index);
        }

        return removedElement;
    }

    public int get(int index) {
        checkIndex(index);
        return getUnchecked(index);
    }

    public int getUnchecked(int index) {
        return backingArray[index];
    }

    public void set(int index, int newValue) {
        checkIndex(index);
        setUnchecked(index, newValue);
    }

    public void setUnchecked(int index, int newValue) {
        backingArray[index] = newValue;
    }

    public void clear() {
        numElements = 0;
    }

    public int[] getBackingArray() {
        return backingArray;
    }

    public void sort() {
        Arrays.sort(backingArray, 0, numElements);
    }

    public boolean ensureCapacity(int targetCapacity) {
        return ensureCapacity((long) targetCapacity);
    }

    @Override
    public String toString() {
        return "IntArrayList{" +
                "backingArray=" + Arrays.toString(backingArray) +
                ", numElements=" + numElements +
                '}';
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= numElements) {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    private void ensureCanAddNewElement() {
        ensureCanAddNElements(1);
    }

    private void ensureCanAddNElements(int numNewElements) {
        int newTargetSize;

        if (backingArray == null) {
            newTargetSize = Math.max(numNewElements, INITIAL_SIZE);
        } else if (backingArray.length < numElements + numNewElements) {
            newTargetSize = (backingArray.length + numNewElements) << 1;
        } else {
            return;
        }

        ensureCapacity(newTargetSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntArrayList integers = (IntArrayList) o;

        if (numElements != integers.numElements) return false;

        for (int i = 0; i < numElements; ++i) {
            if (backingArray[i] != integers.backingArray[i]) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = numElements;

        for (int i = 0; i < numElements; ++i) {
            result = 31 * result + backingArray[i];
        }

        return result;
    }
}