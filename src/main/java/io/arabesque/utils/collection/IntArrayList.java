package io.arabesque.utils.collection;

import io.arabesque.utils.pool.IntArrayListPool;
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
import java.util.NoSuchElementException;

public class IntArrayList implements ReclaimableIntCollection, Writable {
    private static final int INITIAL_SIZE = 16;

    private int[] backingArray;
    private int numElements;
    private boolean preventReclaim = false;
    private IntConsumer intAdder;

    public IntArrayList() {
        this(16);
    }

    public IntArrayList(int capacity) {
        ensureCapacity(capacity);
        this.numElements = 0;
    }

    public IntArrayList(boolean preventReclaim) {
        this();
        this.preventReclaim = preventReclaim;
    }

    public IntArrayList(Collection<Integer> collection) {
        this(collection.size());
        addAll(collection);
    }

    public IntArrayList(IntArrayList intArrayList) {
        this(intArrayList.backingArray, intArrayList.numElements);
    }

    public IntArrayList(int[] intArray, int numElements) {
        this.numElements = numElements;
        backingArray = Arrays.copyOf(intArray, numElements);
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

        int minimumSize = (int) l;

        if (backingArray == null) {
            backingArray = new int[minimumSize];
        }
        else if (minimumSize > backingArray.length) {
            int targetLength = Math.max(backingArray.length, 1);

            while (targetLength < minimumSize) {
                targetLength = targetLength << 1;

                if (targetLength < 0) {
                    targetLength = minimumSize;
                    break;
                }
            }

            backingArray = Arrays.copyOf(backingArray, targetLength);
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

        for (int i = 0; i < numElements; ++i) {
            ts[i] = (T) Integer.valueOf(backingArray[i]);
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

    @Override
    public void reclaim() {
        if (preventReclaim) {
            return;
        }

        IntArrayListPool.instance().reclaimObject(this);
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
            if (index >= numElements - 1) {
                throw new NoSuchElementException();
            }

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
        if (c == null || c.size() == 0) {
            return false;
        }

        ensureCanAddNElements(c.size());

        for (int o : c) {
            add(o);
        }

        return true;
    }

    public boolean addAll(IntCollection c) {
        if (c == null || c.size() == 0) {
            return false;
        }

        ensureCanAddNElements(c.size());

        if (intAdder == null) {
            intAdder = new IntConsumer() {
                @Override
                public void accept(int i) {
                    add(i);
                }
            };
        }

        c.forEach(intAdder);

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

        for (int i = numElements - 1; i >= 0; --i) {
            int e = backingArray[i];

            boolean collectionContainsE = c.contains(e);

            if (ifPresent == collectionContainsE) {
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
        StringBuilder strBuilder = new StringBuilder();

        strBuilder.append("IntArrayList{");
        strBuilder.append("backingArray=");

        boolean first = true;

        for (int i = 0; i < numElements; ++i) {
            if (!first) {
                strBuilder.append(", ");
            }

            strBuilder.append(backingArray[i]);

            first = false;
        }

        strBuilder.append(", numElements=");
        strBuilder.append(numElements);
        strBuilder.append("}");

        return strBuilder.toString();
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= numElements) {
            throw new ArrayIndexOutOfBoundsException(index);
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
            newTargetSize = getSizeWithPaddingWithoutOverflow(numNewElements, numElements + numNewElements);
        } else {
            return;
        }

        ensureCapacity(newTargetSize);
    }

    private int getSizeWithPaddingWithoutOverflow(int targetSize, int currentSize) {
        if (currentSize > targetSize) {
            return currentSize;
        }

        int sizeWithPadding = Math.max(currentSize, 1);

        while (true) {
            int previousSizeWithPadding = sizeWithPadding;

            // Multiply by 2
            sizeWithPadding <<= 1;

            // If we saw an overflow, return simple targetSize
            if (previousSizeWithPadding > sizeWithPadding) {
                return targetSize;
            }

            if (sizeWithPadding >= targetSize) {
                return sizeWithPadding;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntArrayList integers = (IntArrayList) o;

        return equals(integers);
    }

    public boolean equals(IntArrayList intArrayList) {
        if (this == intArrayList) return true;
        if (intArrayList == null) return false;

        if (numElements != intArrayList.numElements) return false;

        for (int i = 0; i < numElements; ++i) {
            if (backingArray[i] != intArrayList.backingArray[i]) {
                return false;
            }
        }

        return true;
    }

    public boolean equalsCollection(Collection<Integer> intCollection) {
        if (this == intCollection) return true;
        if (intCollection == null) return false;

        if (numElements != intCollection.size()) return false;

        int i = 0;
        for (Integer e : intCollection) {
            if (backingArray[i] != e) {
                return false;
            }

            ++i;
        }

        return true;
    }

    public boolean equalsIntCollection(IntCollection intCollection) {
        if (this == intCollection) return true;
        if (intCollection == null) return false;

        if (numElements != intCollection.size()) return false;

        IntCursor intCursor = intCollection.cursor();

        int i = 0;
        while (intCursor.moveNext()) {
            if (backingArray[i] != intCursor.elem()) {
                return false;
            }

            ++i;
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

    public int pop() {
        return remove(numElements - 1);
    }

    public void removeLast() {
        removeLast(1);
    }

    public void removeLast(int n) {
        numElements = Math.max(0, numElements - n);
    }

    public int getLast() {
        int index = numElements - 1;

        if (index >= 0) {
            return backingArray[index];
        }
        else {
            throw new ArrayIndexOutOfBoundsException(index);
        }
    }

    public int getLastOrDefault(int def) {
        int index = numElements - 1;

        if (index >= 0) {
            return backingArray[index];
        }
        else {
            return def;
        }
    }

    public int findLargestCommonPrefixEnd(IntArrayList other) {
        if (other == null) {
            return 0;
        }

        int pos;
        int minPos = Math.min(size(), other.size());

        for (pos = 0; pos < minPos; ++pos) {
            if (getUnchecked(pos) != other.getUnchecked(pos)) {
                return pos;
            }
        }

        return pos;
    }
}