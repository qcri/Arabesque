package io.arabesque.pattern;

import io.arabesque.utils.WritableObjArrayList;

import java.util.Collection;

/**
 * Created by Alex on 28-Oct-15.
 */
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
        return new PatternEdge();
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
