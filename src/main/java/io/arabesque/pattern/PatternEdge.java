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
    private boolean type; //true : forward , false : backward

    public PatternEdge() {
        type = true;
    }

    public PatternEdge(PatternEdge edge) {
        this(edge.getSrcId(), edge.getSrcLabel(), edge.getDestId(), edge.getDestLabel(), edge.getType());
    }

    public PatternEdge(int srcId, int srcLabel, int destId, int destLabel) {
        this.srcId = srcId;
        this.srcLabel = srcLabel;
        this.destId = destId;
        this.destLabel = destLabel;
        type = true;
    }

    public PatternEdge(int srcId, int srcLabel, int destId, int destLabel, boolean type) {
        this.srcId = srcId;
        this.srcLabel = srcLabel;
        this.destId = destId;
        this.destLabel = destLabel;
        this.type = type;
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

    public boolean getType() {
        return type;
    }

    public void setType(boolean type) {
        this.type = type;
    }


    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append("[" + srcId + "," + srcLabel + "-" + destId + "," + destLabel + "-" + type + "]");

        return result.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.srcId);
        out.writeInt(this.srcLabel);
        out.writeInt(this.destId);
        out.writeInt(this.destLabel);
        out.writeBoolean(this.type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.srcId = in.readInt();
        this.srcLabel = in.readInt();
        this.destId = in.readInt();
        this.destLabel = in.readInt();
        this.type = in.readBoolean();
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
            if (this.type == true && e.getType() == true) {
                if (this.destId < e.getDestId())
                    isSmaller = true;
                else if (this.destId == e.getDestId()) {
                    if (this.srcId > e.getSrcId())
                        isSmaller = true;
                }
            }
            //bwd, bwd
            else if (this.type == false && e.getType() == false) {
                if (this.srcId < e.getSrcId())
                    isSmaller = true;
                if (this.srcId == e.getSrcId()) {
                    if (this.destId < e.getDestId())
                        isSmaller = true;
                }
            }

            //fwd, bwd
            else if (this.type == true && e.getType() == false) {
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
        PatternEdge e = (PatternEdge) o;
        if (this.srcId == e.getSrcId() &&
                this.srcLabel == e.getSrcLabel() &&
                this.destId == e.getDestId() &&
                this.destLabel == e.getDestLabel())
            return true;
        return false;

    }

    @Override
    public int hashCode() {
        int result = srcId;
        result = 31 * result + srcLabel;
        result = 31 * result + destId;
        result = 31 * result + destLabel;
        return result;
    }

    @Override
    public int compareTo(PatternEdge o) {
        if (equals(o))
            return 0;
        else if (isSmaller(o)) return -1;
        else return 1;

    }

    ;

    public int getTypeInt() {
        if (type) {
            return 1;
        }
        return 0;
    }
}