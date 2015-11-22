package io.arabesque.pattern;

import io.arabesque.graph.Edge;
import io.arabesque.graph.LabelledEdge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Alex on 08-Nov-15.
 */
public class LabelledPatternEdge extends PatternEdge {
    private int label;

    public LabelledPatternEdge() {
        label = 0;
    }

    public LabelledPatternEdge(LabelledPatternEdge edge) {
        super(edge);

        this.label = edge.label;
    }

    public LabelledPatternEdge(int srcPos, int srcLabel, int destPos, int destLabel, int label) {
        super(srcPos, srcLabel, destPos, destLabel);

        this.label = label;
    }

    @Override
    public void setFromOther(PatternEdge edge) {
        super.setFromOther(edge);

        if (edge instanceof LabelledPatternEdge) {
            label = ((LabelledPatternEdge) edge).label;
        }
    }

    @Override
    public void setFromEdge(Edge edge, int srcPos, int dstPos, int srcId) {
        super.setFromEdge(edge, srcPos, dstPos, srcId);

        if (edge instanceof LabelledEdge) {
            label = ((LabelledEdge) edge).getEdgeLabel();
        }
    }

    public int getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LabelledPatternEdge that = (LabelledPatternEdge) o;

        return label == that.label;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + label;
        return result;
    }

    @Override
    public int compareTo(PatternEdge o) {
        int result = super.compareTo(o);

        if (result != 0) {
            return result;
        }

        if (o instanceof LabelledPatternEdge) {
            LabelledPatternEdge lo = (LabelledPatternEdge) o;
            return Integer.compare(label, lo.label);
        }

        return 0;
    }

    @Override
    public String toString() {
        return ("[" + getSrcPos() + "(" + getSrcLabel() + ")--(" + label + ")--" + getDestPos() + "(" + getDestLabel() + ")]");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(label);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        label = in.readInt();
    }
}
