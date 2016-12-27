package io.arabesque.utils.collection;

public interface ReclaimableObjCollection<O> extends com.koloboke.collect.ObjCollection<O> {
    void reclaim();
}
