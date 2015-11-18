package io.arabesque.pattern;

import io.arabesque.pattern.pool.PatternEdgeArrayListPool;
import io.arabesque.pattern.pool.PatternEdgePool;
import io.arabesque.utils.WritableObjArrayList;

import java.util.Collection;

public class PatternEdgeArrayList extends WritableObjArrayList<PatternEdge> implements Comparable<PatternEdgeArrayList> {
    public PatternEdgeArrayList() {
    }

    public PatternEdgeArrayList(int capacity) {
        super(capacity);
    }

    public PatternEdgeArrayList(Collection<PatternEdge> elements) {
        super(elements);
    }

    @Override
    protected PatternEdge createObject() {
        return PatternEdgePool.instance().createObject();
    }

    @Override
    public void reclaim() {
        PatternEdgeArrayListPool.instance().reclaimObject(this);
    }

    public int compareTo(PatternEdgeArrayList other) {
        int mySize = size();
        int otherSize = other.size();

        if (mySize == otherSize) {
            for (int i = 0; i < mySize; ++i) {
                PatternEdge myPatternEdge = get(i);
                PatternEdge otherPatternEdge = other.get(i);

                int comparisonResult = myPatternEdge.compareTo(otherPatternEdge);

                if (comparisonResult != 0) {
                    return comparisonResult;
                }
            }
        }
        else {
            return mySize - otherSize;
        }

        return 0;
    }
}
