package io.arabesque.pattern;

import io.arabesque.utils.WritableObjArrayList;

import java.util.Collection;

/**
 * Created by Alex on 28-Oct-15.
 */
public class PatternEdgeArrayList extends WritableObjArrayList<PatternEdge> {
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
}
