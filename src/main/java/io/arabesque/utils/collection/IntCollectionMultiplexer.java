package io.arabesque.utils.collection;

import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.IntIterator;
import net.openhft.koloboke.collect.ObjCollection;
import net.openhft.koloboke.function.IntConsumer;
import net.openhft.koloboke.function.IntPredicate;

import javax.annotation.Nonnull;
import java.util.Collection;

public class IntCollectionMultiplexer implements IntCollection {
    private ObjArrayList<IntCollection> collections;
    private int totalSize;

    public IntCollectionMultiplexer() {
        totalSize = 0;
        collections = new ObjArrayList<>();
    }

    public IntCollectionMultiplexer(IntCollection... collections) {
        this();

        addCollections(collections);
    }

    public void addCollection(IntCollection collection) {
        collections.add(collection);
        int previousSize = totalSize;
        totalSize += collection.size();

        if (previousSize > totalSize) {
            throw new IllegalStateException("Total collection size overflows");
        }
    }

    public void addCollections(ObjCollection<? extends IntCollection> collections) {
        for (IntCollection collection : collections) {
            addCollection(collection);
        }
    }

    public void addCollections(IntCollection... collections) {
        for (int i = 0; i < collections.length; ++i) {
            addCollection(collections[i]);
        }
    }

    @Override
    public int size() {
        return totalSize;
    }

    @Override
    public long sizeAsLong() {
        return totalSize;
    }

    @Override
    public boolean ensureCapacity(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shrink() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return totalSize == 0;
    }

    @Override
    public boolean contains(Object o) {
        for (int i = 0; i < collections.size(); ++i) {
            if (collections.get(i).contains(o)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean contains(int value) {
        for (int i = 0; i < collections.size(); ++i) {
            if (collections.get(i).contains(value)) {
                return true;
            }
        }

        return false;
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public int[] toIntArray() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public int[] toArray(@Nonnull int[] ints) {
        throw new UnsupportedOperationException();
    }

    public class IntCollectionMultiplexerCursor implements IntCursor {
        private int collectionIdx = -1;
        private IntCursor currentCursor = null;

        @Override
        public void forEachForward(@Nonnull IntConsumer intConsumer) {
            while (moveNext()) {
                intConsumer.accept(elem());
            }
        }

        @Override
        public int elem() {
            return currentCursor.elem();
        }

        @Override
        public boolean moveNext() {
            if (collectionIdx >= collections.size()) {
                return false;
            }

            while (true) {
                if (currentCursor == null) {
                    ++collectionIdx;

                    if (collectionIdx >= collections.size()) {
                        return false;
                    }

                    currentCursor = collections.get(collectionIdx).cursor();
                }

                if (currentCursor.moveNext()) {
                    return true;
                } else {
                    currentCursor = null;
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Nonnull
    @Override
    public IntCursor cursor() {
        return new IntCollectionMultiplexerCursor();
    }

    public class IntCollectionMultiplexerIterator implements IntIterator {
        private int collectionIdx = -1;
        private IntIterator currentIterator = null;

        @Override
        public int nextInt() {
            return currentIterator.nextInt();
        }

        @Override
        public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
            while (hasNext()) {
                intConsumer.accept(next());
            }
        }

        @Override
        public boolean hasNext() {
            if (collectionIdx >= collections.size()) {
                return false;
            }

            while (true) {
                if (currentIterator == null) {
                    ++collectionIdx;

                    if (collectionIdx >= collections.size()) {
                        return false;
                    }

                    currentIterator = collections.get(collectionIdx).iterator();
                }

                if (currentIterator.hasNext()) {
                    return true;
                } else {
                    currentIterator = null;
                }
            }
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
    public IntIterator iterator() {
        return new IntCollectionMultiplexerIterator();
    }

    @Override
    public void forEach(@Nonnull IntConsumer intConsumer) {
        IntCursor intCursor = cursor();

        while (intCursor.moveNext()) {
            intConsumer.accept(intCursor.elem());
        }
    }

    @Override
    public boolean forEachWhile(@Nonnull IntPredicate intPredicate) {
        IntCursor intCursor = cursor();

        while (intCursor.moveNext()) {
            if (!intPredicate.test(intCursor.elem())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean add(@Nonnull Integer integer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        totalSize = 0;
        collections.clear();
    }

    @Override
    public boolean removeInt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(@Nonnull IntPredicate intPredicate) {
        throw new UnsupportedOperationException();
    }
}
