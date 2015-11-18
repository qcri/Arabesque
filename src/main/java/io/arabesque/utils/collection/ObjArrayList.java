package io.arabesque.utils.collection;

import net.openhft.koloboke.collect.Equivalence;
import net.openhft.koloboke.collect.ObjCursor;
import net.openhft.koloboke.collect.ObjIterator;
import net.openhft.koloboke.function.Consumer;
import net.openhft.koloboke.function.Predicate;
import org.apache.commons.collections.ComparatorUtils;

import javax.annotation.Nonnull;
import java.util.*;

public class ObjArrayList<O> implements ReclaimableObjCollection<O>, List<O> {
    private ArrayList<O> backingArray;

    public ObjArrayList() {
        backingArray = new ArrayList<>();
    }

    public ObjArrayList(int capacity) {
        backingArray = new ArrayList<>(capacity);
    }

    public ObjArrayList(Collection<O> elements) {
        backingArray = new ArrayList<>(elements);
    }

    @Nonnull
    @Override
    public Equivalence<O> equivalence() {
        return Equivalence.defaultEquality();
    }

    @Override
    public void forEach(@Nonnull Consumer<? super O> consumer) {
        for (O element : backingArray) {
            consumer.accept(element);
        }
    }

    @Override
    public boolean forEachWhile(@Nonnull Predicate<? super O> predicate) {
        for (O element : backingArray) {
            if (!predicate.test(element)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void reclaim() {
        // Do nothing by default since we don't have a pool of generic objects.
    }

    public class ObjArrayListCursor implements ObjCursor<O> {
        private ListIterator<O> backingListIterator;
        private O currentElement;

        public ObjArrayListCursor(ListIterator<O> listIterator) {
            backingListIterator = listIterator;
        }

        @Override
        public void forEachForward(@Nonnull Consumer<? super O> consumer) {
            while (moveNext()) {
                consumer.accept(elem());
            }
        }

        @Override
        public O elem() {
            return currentElement;
        }

        @Override
        public boolean moveNext() {
            if (backingListIterator.hasNext()) {
                currentElement = backingListIterator.next();
                return true;
            } else {
                currentElement = null;
                return false;
            }
        }

        @Override
        public void remove() {
            backingListIterator.remove();
        }
    }

    @Nonnull
    @Override
    public ObjCursor<O> cursor() {
        return new ObjArrayListCursor(backingArray.listIterator());
    }

    @Override
    public int size() {
        return backingArray.size();
    }

    @Override
    public long sizeAsLong() {
        return backingArray.size();
    }

    // NOTE: Breaks Collection.ensureCapacity interface in that it always returns true
    @Override
    public boolean ensureCapacity(long l) {
        if (l > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("ObjArrayList does not support long sizes yet");
        }

        int intCapacity = (int) l;

        backingArray.ensureCapacity(intCapacity);

        return true;
    }

    @Override
    public boolean shrink() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return backingArray.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return backingArray.contains(o);
    }

    public class ObjArrayListIterator<O> implements ObjIterator<O> {
        private ListIterator<O> backingListIterator;

        public ObjArrayListIterator(ListIterator<O> backingListIterator) {
            this.backingListIterator = backingListIterator;
        }

        @Override
        public void forEachRemaining(@Nonnull Consumer<? super O> consumer) {
            while (backingListIterator.hasNext()) {
                consumer.accept(backingListIterator.next());
            }
        }

        @Override
        public boolean hasNext() {
            return backingListIterator.hasNext();
        }

        @Override
        public O next() {
            return backingListIterator.next();
        }

        @Override
        public void remove() {
            backingListIterator.remove();
        }
    }

    @Nonnull
    @Override
    public ObjIterator<O> iterator() {
        return new ObjArrayListIterator<>(backingArray.listIterator());
    }

    @Override
    public Object[] toArray() {
        return backingArray.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return backingArray.toArray(a);
    }

    @Override
    public boolean add(O o) {
        return backingArray.add(o);
    }

    @Override
    public boolean remove(Object o) {
        return backingArray.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return backingArray.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends O> c) {
        return backingArray.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends O> c) {
        return backingArray.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return backingArray.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return backingArray.retainAll(c);
    }

    @Override
    public void clear() {
        backingArray.clear();
    }

    @Override
    public boolean removeIf(@Nonnull Predicate<? super O> predicate) {
        boolean removedAtLeastOne = false;
        Iterator<O> iterator = backingArray.iterator();

        while (iterator.hasNext()) {
            if (predicate.test(iterator.next())) {
                removedAtLeastOne = true;
                iterator.remove();
            }
        }

        return removedAtLeastOne;
    }

    @Override
    public String toString() {
        return "ObjArrayList{" +
                backingArray +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjArrayList<?> that = (ObjArrayList<?>) o;

        return backingArray.equals(that.backingArray);

    }

    @Override
    public int hashCode() {
        return backingArray.hashCode();
    }

    @Override
    public O get(int index) {
        return backingArray.get(index);
    }

    @Override
    public O set(int index, O element) {
        return backingArray.set(index, element);
    }

    @Override
    public void add(int index, O element) {
        backingArray.add(index, element);
    }

    @Override
    public O remove(int index) {
        return backingArray.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return backingArray.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return backingArray.lastIndexOf(o);
    }

    @Override
    public ListIterator<O> listIterator() {
        return backingArray.listIterator();
    }

    @Override
    public ListIterator<O> listIterator(int index) {
        return backingArray.listIterator(index);
    }

    @Override
    public List<O> subList(int fromIndex, int toIndex) {
        return backingArray.subList(fromIndex, toIndex);
    }

    public void sort(Comparator<? super O> comparator) {
        Collections.sort(backingArray, comparator);
    }

    public void sort() {
        sort(ComparatorUtils.naturalComparator());
    }

    public O pop() {
        return remove(size() - 1);
    }
}
