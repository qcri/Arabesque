package io.arabesque.pattern;

import io.arabesque.utils.WritableObjArrayList;

/**
 * Created by Alex on 28-Oct-15.
 */
public class PatternEdgeArrayList extends WritableObjArrayList<PatternEdge> {
    @Override
    protected PatternEdge createObject() {
        return new PatternEdge();
    }
}
