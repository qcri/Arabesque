package io.arabesque.utils;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;

public class ReadIntegerIterable implements Iterable<Integer> {
    private DataInput dataInput;
    private int numIntegersToRead;
    private int numIntegersRead;

    public ReadIntegerIterable(DataInput dataInput, int numIntegersToRead) {
        this.dataInput = dataInput;
        this.numIntegersToRead = numIntegersToRead;
        this.numIntegersRead = 0;
    }

    @Override
    public Iterator<Integer> iterator() {
        return new ReadIntegerIterator();
    }

    private class ReadIntegerIterator implements Iterator<Integer> {
        @Override
        public boolean hasNext() {
            return numIntegersRead < numIntegersToRead;
        }

        @Override
        public Integer next() {
            try {
                ++numIntegersRead;
                return dataInput.readInt();
            } catch (IOException e) {
                throw new NoMoreDataException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public class NoMoreDataException extends RuntimeException {
    }
}
