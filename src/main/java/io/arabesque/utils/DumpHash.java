package io.arabesque.utils;


import java.util.Arrays;

/**
 * The assumption of this class is that the hashmap will be always very small.
 * Need to have a parameter whether to use it or not.
 */
public class DumpHash {
    int[] keys;
    int[] values;
    int index = 0;

    public DumpHash(int size) {
        keys = new int[size];
        values = new int[size];
        index = 0;
    }

    public int get(int key) {
        int i = 0;
        while (i < index) {
            if (keys[i] == key) {
                return values[i];
            }
            ++i;
        }
        return -1;
    }

    public void put(int key, int value) {
        if (index >= keys.length) {
            System.out.println("Check me: Growing Dump buffer to:" + keys.length * 2);
            keys = Arrays.copyOf(keys, keys.length * 2);
            values = Arrays.copyOf(values, values.length * 2);
        }

        keys[index] = key;
        values[index] = value;
        ++index;
    }

    @Override
    public String toString() {
        return "DumpHash{" +
                "keys=" + Arrays.toString(keys) +
                ", values=" + Arrays.toString(values) +
                ", index=" + index +
                '}';
    }

    public void setIndex(int value) {
        index = value;
    }

    public void clear() {
        index = 0;
    }
}
