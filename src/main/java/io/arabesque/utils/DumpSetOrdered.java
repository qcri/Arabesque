package io.arabesque.utils;

import java.util.Arrays;

/**
 * For small sets, fifo kind of set.
 */
public class DumpSetOrdered {
    int[] keys;
    int index = 0;

    public DumpSetOrdered(int size) {
        keys = new int[size];
    }

    public DumpSetOrdered(int[] vertices, int numVertices) {
        keys = vertices;
        index = numVertices;
    }

    /**
     * Return true if the key was added.
     *
     * @param key
     * @return
     */
    public boolean add(int key) {
        int i = 0;
        while (i < index) {
            if (keys[i] == key) {
                // key existed
                return false;
            }
            ++i;
        }
        // key didn't exist so we have to add it.

        if (index >= keys.length) {
            System.out.println("Check me: Growing Dump Set Ordered to:" + keys.length * 2);
            keys = Arrays.copyOf(keys, keys.length * 2);
        }
        keys[index] = key;
        ++index;
        return true;
    }

    public void remove(int size) {
        index -= size;
    }

    public int[] getKeys() {
        return keys;
    }

    public int getNumKeys() {
        return index;
    }

    public void reset() {
        index = 0;
    }

    public void replaceWith(int[] vertices, int numVertices) {
        keys = vertices;
        index = numVertices;
    }

    @Override
    public String toString() {

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; i < index; i++) {
            b.append(keys[i] + ", ");
        }
        return b.append(']').toString();
    }
}
