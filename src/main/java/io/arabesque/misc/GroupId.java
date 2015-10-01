package io.arabesque.misc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public abstract class GroupId implements Writable {
    public static final Comparator<GroupId> COMPARATOR = new Comparator<GroupId>() {
        @Override
        public int compare(GroupId o1, GroupId o2) {
            return Integer.compare(o1.getDepth(), o2.getDepth());
        }
    };

    private int depth;
    private int numEmbeddings;

    public GroupId() {
        depth = 0;
        numEmbeddings = 0;
    }

    public GroupId(int depth) {
        this.depth = depth;
        numEmbeddings = 0;
    }

    public GroupId(GroupId other) {
        this.depth = other.depth;
        this.numEmbeddings = other.numEmbeddings;
    }

    public int getDepth() {
        return depth;
    }

    public abstract GroupId copy();


    @Override
    public String toString() {
        return "GroupId{" +
                "depth=" + depth +
                "numEmbeddings=" + numEmbeddings +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(depth);
        dataOutput.writeInt(numEmbeddings);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        depth = dataInput.readInt();
        numEmbeddings = dataInput.readInt();
    }

}
