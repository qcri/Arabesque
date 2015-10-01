package io.arabesque.writables;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CharWritable implements WritableComparable<CharWritable> {
    private char value;

    public CharWritable() {
    }

    public CharWritable(char value) {
        this.set(value);
    }

    public void set(char value) {
        this.value = value;
    }

    public char get() {
        return this.value;
    }

    public void readFields(DataInput in) throws IOException {
        this.value = Character.toChars(in.readInt())[0];
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.value);
    }

    public boolean equals(Object o) {
        if (!(o instanceof CharWritable)) {
            return false;
        } else {
            CharWritable other = (CharWritable) o;
            return this.value == other.value;
        }
    }

    public int hashCode() {
        return this.value;
    }

    public int compareTo(CharWritable o) {
        char thisValue = this.value;
        char thatValue = o.value;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Character.toString(this.value);
    }

    static {
        WritableComparator.define(CharWritable.class, new CharWritable.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(CharWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}
