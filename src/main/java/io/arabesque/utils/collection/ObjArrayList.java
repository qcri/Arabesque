package io.arabesque.utils.collection;

import net.openhft.koloboke.collect.Equivalence;
import net.openhft.koloboke.collect.ObjCollection;
import net.openhft.koloboke.collect.ObjCursor;
import net.openhft.koloboke.collect.ObjIterator;
import net.openhft.koloboke.function.Consumer;
import net.openhft.koloboke.function.Predicate;

import javax.annotation.Nonnull;
import java.util.*;

public class ObjArrayList<O> implements ReclaimableObjCollection<O>, List<O> {
    private static final int INITIAL_SIZE = 16;

    private O[] backingArray;
    private int numElements;
    private boolean preventReclaim = false;
    private Consumer<O> objAdder;

    public ObjArrayList() {
        this(16);
    }

    public ObjArrayList(int capacity) {
        ensureCapacity(capacity);
        this.numElements = 0;
    }

    public ObjArrayList(boolean preventReclaim) {
        this();
        this.preventReclaim = preventReclaim;
    }

    public ObjArrayList(Collection<O> collection) {
        this(collection.size());
        addAll(collection);
    }

    public ObjArrayList(ObjArrayList<O> objArrayList) {
        this(objArrayList.backingArray, objArrayList.numElements);
    }

    public ObjArrayList(O[] objArray, int numElements) {
        this.numElements = numElements;
        backingArray = Arrays.copyOf(objArray, numElements);
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
            backingArray = (O[]) new Object[minimumSize];
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
    public boolean contains(Object element) {
        for (int i = 0; i < numElements; ++i) {
            if (backingArray[i].equals(element)) {
                return true;
            }
        }

        return false;
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        return toArray(new Object[numElements]);
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts) {
        if (ts.length < numElements) {
            ts = Arrays.copyOf(ts, numElements);
        }

        for (int i = 0; i < numElements; ++i) {
            ts[i] = (T) backingArray[i];
        }

        if (ts.length > numElements) {
            ts[numElements] = null;
        }

        return ts;
    }

    @Override
    public void reclaim() {

    }

    private class ObjArrayListCursor implements ObjCursor<O> {
        private int index;

        public ObjArrayListCursor() {
            this.index = -1;
        }

        @Override
        public void forEachForward(@Nonnull Consumer<? super O> consumer) {
            int localNumElements = numElements;

            for (int i = index; i < localNumElements; ++i) {
                consumer.accept(backingArray[i]);
            }

            if(localNumElements != numElements) {
                throw new ConcurrentModificationException();
            } else {
                this.index = numElements;
            }
        }

        @Override
        public O elem() {
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

    private class ObjArrayListIterator implements ObjIterator<O> {
        private int index;

        public ObjArrayListIterator() {
            this.index = -1;
        }

        @Override
        public void forEachRemaining(@Nonnull Consumer<? super O> consumer) {
            int localNumElements = numElements;

            for (int i = index + 1; i < localNumElements - 1; ++i) {
                consumer.accept(backingArray[i]);
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
        public O next() {
            if (index >= numElements - 1) {
                throw new NoSuchElementException();
            }

            return backingArray[++index];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Nonnull
    @Override
    public ObjCursor<O> cursor() {
        return new ObjArrayListCursor();
    }

    @Nonnull
    @Override
    public ObjIterator<O> iterator() {
        return new ObjArrayListIterator();
    }

    @Nonnull
    @Override
    public Equivalence<O> equivalence() {
        return null;
    }

    @Override
    public void forEach(@Nonnull Consumer<? super O> consumer) {
        for (int i = 0; i < numElements; ++i) {
            consumer.accept(backingArray[i]);
        }
    }

    @Override
    public boolean forEachWhile(@Nonnull Predicate<? super O> predicate) {
        for (int i = 0; i < numElements; ++i) {
            if (!predicate.test(backingArray[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean add(@Nonnull O obj) {
        ensureCanAddNewElement();
        backingArray[numElements++] = obj;
        return true;
    }

    @Override
    public boolean remove(Object o) {
        for (int i = 0; i < numElements; ++i) {
            O e = backingArray[i];

            if (e.equals(o)) {
                remove(i);
                return true;
            }
        }

        return false;
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
    public boolean addAll(Collection<? extends O> c) {
        if (c == null || c.size() == 0) {
            return false;
        }

        ensureCanAddNElements(c.size());

        for (O o : c) {
            add(o);
        }

        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends O> c) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(ObjCollection<O> c) {
        if (c == null || c.size() == 0) {
            return false;
        }

        ensureCanAddNElements(c.size());

        if (objAdder == null) {
            objAdder = new Consumer<O>() {
                @Override
                public void accept(O o) {
                    add(o);
                }
            };
        }

        c.forEach(objAdder);

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
            O e = backingArray[i];

            boolean collectionContainsE = c.contains(e);

            if (ifPresent == collectionContainsE) {
                remove(i);
                removedAtLeastOne = true;
            }
        }

        return removedAtLeastOne;
    }

    @Override
    public boolean removeIf(@Nonnull Predicate<? super O> predicate) {
        boolean removedAtLeastOne = false;

        for (int i = 0; i < numElements; ++i) {
            if (predicate.test(backingArray[i])) {
                removedAtLeastOne = true;
                remove(i);
            }
        }

        return removedAtLeastOne;
    }

    public O remove(int index) {
        if (index < 0 || index >= numElements) {
            throw new IllegalArgumentException();
        }

        O removedElement = backingArray[index];

        --numElements;

        if (index != numElements) {
            System.arraycopy(backingArray, index + 1, backingArray, index, numElements - index);
        }

        return removedElement;
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<O> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<O> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<O> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    public O get(int index) {
        /*checkIndex(index);
        return getUnchecked(index);*/
        return backingArray[index];
    }

    public O getUnchecked(int index) {
        return backingArray[index];
    }

    public O set(int index, O newValue) {
        checkIndex(index);
        return setUnchecked(index, newValue);
    }

    @Override
    public void add(int index, O element) {
        throw new UnsupportedOperationException();
    }

    public O setUnchecked(int index, O newValue) {
        O existingElement = backingArray[index];
        backingArray[index] = newValue;
        return existingElement;
    }

    public void clear() {
        numElements = 0;
    }

    public O[] getBackingArray() {
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

        strBuilder.append("ObjArrayList{");
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

        ObjArrayList integers = (ObjArrayList) o;

        return equals(integers);
    }

    public boolean equals(ObjArrayList objArrayList) {
        if (this == objArrayList) return true;
        if (objArrayList == null) return false;

        if (numElements != objArrayList.numElements) return false;

        for (int i = 0; i < numElements; ++i) {
            if (!backingArray[i].equals(objArrayList.backingArray[i])) {
                return false;
            }
        }

        return true;
    }

    public boolean equalsCollection(Collection<? extends O> collection) {
        if (this == collection) return true;
        if (collection == null) return false;

        if (numElements != collection.size()) return false;

        int i = 0;
        for (O e : collection) {
            if (!backingArray[i].equals(e)) {
                return false;
            }

            ++i;
        }

        return true;
    }

    public boolean equalsObjCollection(ObjCollection<? extends O> collection) {
        if (this == collection) return true;
        if (collection == null) return false;

        if (numElements != collection.size()) return false;

        ObjCursor<? extends O> objCursor = collection.cursor();

        int i = 0;
        while (objCursor.moveNext()) {
            if (!backingArray[i].equals(objCursor.elem())) {
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
            result = 31 * result + backingArray[i].hashCode();
        }

        return result;
    }

    public O pop() {
        return remove(numElements - 1);
    }

    public void removeLast() {
        removeLast(1);
    }

    public void removeLast(int n) {
        int stoppingIndex = Math.max(0, numElements - n);

        for (int i = numElements - 1; i >= stoppingIndex; --i) {
            remove(i);
        }
    }

    public O getLast() {
        int index = numElements - 1;

        if (index >= 0) {
            return backingArray[index];
        }
        else {
            throw new ArrayIndexOutOfBoundsException(index);
        }
    }

    public O getLastOrDefault(O def) {
        int index = numElements - 1;

        if (index >= 0) {
            return backingArray[index];
        }
        else {
            return def;
        }
    }

    public int findLargestCommonPrefixEnd(ObjArrayList other) {
        if (other == null) {
            return 0;
        }

        int pos;
        int minPos = Math.min(size(), other.size());

        for (pos = 0; pos < minPos; ++pos) {
            if (get(pos) != other.get(pos)) {
                return pos;
            }
        }

        return pos;
    }
}
