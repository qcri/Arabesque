package io.arabesque.utils.collection;

import io.arabesque.utils.pool.IntSingletonPool;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.IntIterator;
import net.openhft.koloboke.function.IntConsumer;
import net.openhft.koloboke.function.IntPredicate;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

public class IntSingleton implements ReclaimableIntCollection {
    private int value;

    public IntSingleton(int value) {
        this.value = value;
    }

    public IntSingleton() {
        value = 0;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public long sizeAsLong() {
        return 1;
    }

    @Override
    public boolean ensureCapacity(long l) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shrink() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return o instanceof Integer && o.equals(value);
    }

    @Override
    public boolean contains(int i) {
        return value == i;
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        return new Object[]{value};
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] ts) {
        if (ts.length < 1) {
            ts = Arrays.copyOf(ts, 1);
        }

        ts[0] = (T) Integer.valueOf(value);

        return ts;
    }

    @Nonnull
    @Override
    public int[] toIntArray() {
        return new int[]{value};
    }

    @Nonnull
    @Override
    public int[] toArray(@Nonnull int[] ints) {
        if (ints.length < 1) {
            return Arrays.copyOf(ints, 1);
        }

        ints[0] = value;

        return ints;
    }

    @Override
    public void reclaim() {
        IntSingletonPool.instance().reclaimObject(this);
    }

    public class IntSingletonCursor implements IntCursor {
        private int numMoves = 0;

        @Override
        public void forEachForward(@Nonnull IntConsumer intConsumer) {
            if (numMoves == 0) {
                intConsumer.accept(value);
                ++numMoves;
            }
        }

        @Override
        public int elem() {
            if (numMoves == 1) {
                return value;
            }

            throw new IllegalStateException();
        }

        @Override
        public boolean moveNext() {
            return numMoves++ == 0;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Nonnull
    @Override
    public IntCursor cursor() {
        return new IntSingletonCursor();
    }

    public class IntSingletonIterator implements IntIterator {
        private int numMoves = 0;

        @Override
        public int nextInt() {
            if (numMoves == 0) {
                ++numMoves;
                return value;
            }
            else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void forEachRemaining(@Nonnull IntConsumer intConsumer) {
            if (numMoves == 0) {
                intConsumer.accept(value);
                ++numMoves;
            }
        }

        @Override
        public boolean hasNext() {
            return numMoves == 0;
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
        return new IntSingletonIterator();
    }

    @Override
    public void forEach(@Nonnull IntConsumer intConsumer) {
        intConsumer.accept(value);
    }

    @Override
    public boolean forEachWhile(@Nonnull IntPredicate intPredicate) {
        return intPredicate.test(value);
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
        throw new UnsupportedOperationException();
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
