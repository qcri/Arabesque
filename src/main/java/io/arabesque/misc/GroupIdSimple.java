package io.arabesque.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by afonseca on 3/20/2015.
 */
public class GroupIdSimple extends GroupId {
    private long id;

    public GroupIdSimple(long id, int depth) {
        super(depth);
        this.id = id;
    }


    public GroupIdSimple(GroupIdSimple other) {
        super(other);
        id = other.id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupIdSimple that = (GroupIdSimple) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString() {
        return "GroupIdSimple{" +
                "id=" + id +
                "} " + super.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeLong(id);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        id = dataInput.readLong();
    }

    @Override
    public GroupId copy() {
        return new GroupIdSimple(this);
    }

}
