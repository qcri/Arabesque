package io.arabesque.utils;

import java.util.Arrays;

public class IntArrayList {
    private static final int INITIAL_SIZE = 16;

    private int[] backingArray;
    private int numElements;

    public IntArrayList() {
        this(16);
    }

    public IntArrayList(int capacity) {
        resizeBackingArray(capacity);
        this.numElements = 0;
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

    public void add(int newValue) {
        ensureCanAddNewElement();
        backingArray[numElements++] = newValue;
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
        Arrays.sort(backingArray);
    }

    public void ensureCapacity(int targetCapacity) {
        if (targetCapacity <= getCapacity()) {
            return;
        }

        resizeBackingArray(targetCapacity);
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

        resizeBackingArray(newTargetSize);
    }

    private void resizeBackingArray(int newTargetSize) {
        if (backingArray == null) {
            backingArray = new int[newTargetSize];
        } else {
            backingArray = Arrays.copyOf(backingArray, newTargetSize);
        }
    }
}