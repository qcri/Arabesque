package io.arabesque.misc;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ObjectId implements WritableComparable {
    long id;
    private int depth;

    public ObjectId() {
    }

    public ObjectId(long id, int depth) {
        this.id = id;
        this.depth = depth;
    }

    public ObjectId(long id, ObjectId parent) {
        this(id, parent != null ? parent.depth + 1 : 0);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public int compareTo(Object o) {
        if (this == o) return 0;
        if (o == null || getClass() != o.getClass()) {
            throw new IllegalArgumentException("Comparing ObjectId with something strange");
        }

        ObjectId that = (ObjectId) o;

        int idCompareResult = Long.compare(id, that.id);

        if (idCompareResult != 0) {
            return idCompareResult;
        }

        return Integer.compare(depth, that.depth);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeInt(depth);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        depth = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectId that = (ObjectId) o;

        if (id != that.id) return false;
        if (depth != that.depth) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + depth;
        return result;
    }

    @Override
    public String toString() {
        return "ObjectId{" +
                "id=" + id +
                ", depth=" + depth +
                '}';
    }
}
