package io.arabesque.pattern;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PatternEdge implements Comparable<PatternEdge>, Writable {

    private int srcId;
    private int srcLabel;
    private int destId;
    private int destLabel;
    private boolean isForward;

    public PatternEdge() {
        this(-1, -1, -1, -1, true);
    }

    public PatternEdge(PatternEdge edge) {
        setFromOther(edge);
    }

    public PatternEdge(int srcId, int srcLabel, int destId, int destLabel) {
        this(srcId, srcLabel, destId, destLabel, true);
    }

    public PatternEdge(int srcId, int srcLabel, int destId, int destLabel, boolean isForward) {
        this.srcId = srcId;
        this.srcLabel = srcLabel;
        this.destId = destId;
        this.destLabel = destLabel;
        this.isForward = isForward;
    }

    public void setFromOther(PatternEdge edge) {
        setSrcId(edge.getSrcId());
        setSrcLabel(edge.getSrcLabel());

        setDestId(edge.getDestId());
        setDestLabel(edge.getDestLabel());

        isForward(edge.isForward());
    }

    public int getSrcId() {
        return srcId;
    }

    public void setSrcId(int srcId) {
        this.srcId = srcId;
    }

    public int getSrcLabel() {
        return srcLabel;
    }

    public void setSrcLabel(int srcLabel) {
        this.srcLabel = srcLabel;
    }

    public int getDestId() {
        return destId;
    }

    public void setDestId(int destId) {
        this.destId = destId;
    }

    public int getDestLabel() {
        return destLabel;
    }

    public void setDestLabel(int destLabel) {
        this.destLabel = destLabel;
    }

    public boolean isForward() {
        return isForward;
    }

    public void isForward(boolean type) {
        this.isForward = type;
    }


    public String toString() {
        return ("[" + srcId + "," + srcLabel + "-" + destId + "," + destLabel + "-" + (isForward ? 'F' : 'B') + "]");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.srcId);
        out.writeInt(this.srcLabel);
        out.writeInt(this.destId);
        out.writeInt(this.destLabel);
        out.writeBoolean(this.isForward);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.srcId = in.readInt();
        this.srcLabel = in.readInt();
        this.destId = in.readInt();
        this.destLabel = in.readInt();
        this.isForward = in.readBoolean();
    }

    public boolean isSmaller(PatternEdge e) {
        boolean isSmaller = false;

        if (this.srcId == e.getSrcId() && this.destId == e.getDestId()) {
            if (this.srcLabel == e.getSrcLabel()) {
                if (this.destLabel < e.getDestId()) {
                    isSmaller = true;
                }
            } else if (this.srcLabel < e.getSrcLabel()) {
                isSmaller = true;
            }
        } else {
            //fwd, fwd
            if (this.isForward && e.isForward()) {
                if (this.destId < e.getDestId())
                    isSmaller = true;
                else if (this.destId == e.getDestId()) {
                    if (this.srcId > e.getSrcId())
                        isSmaller = true;
                }
            }
            //bwd, bwd
            else if (!this.isForward && !e.isForward()) {
                if (this.srcId < e.getSrcId())
                    isSmaller = true;
                if (this.srcId == e.getSrcId()) {
                    if (this.destId < e.getDestId())
                        isSmaller = true;
                }
            }

            //fwd, bwd
            else if (this.isForward && !e.isForward()) {
                if (this.destId <= e.getSrcId()) {
                    isSmaller = true;
                }
            }
            //bwd, fwd
            else {
                if (this.srcId < e.getDestId()) {
                    isSmaller = true;
                }
            }
        }
        return isSmaller;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PatternEdge that = (PatternEdge) o;

        if (srcId != that.srcId) return false;
        if (srcLabel != that.srcLabel) return false;
        if (destId != that.destId) return false;
        if (destLabel != that.destLabel) return false;
        return true;
        //return isForward == that.isForward;

    }

    @Override
    public int hashCode() {
        int result = srcId;
        result = 31 * result + srcLabel;
        result = 31 * result + destId;
        result = 31 * result + destLabel;
        //result = 31 * result + (isForward ? 1 : 0);
        return result;
    }

    @Override
    public int compareTo(PatternEdge o) {
        if (equals(o))
            return 0;
        else if (isSmaller(o)) return -1;
        else return 1;

    }
}