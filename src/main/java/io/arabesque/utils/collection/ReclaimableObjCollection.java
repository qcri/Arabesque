package io.arabesque.utils.collection;

public interface ReclaimableObjCollection<O> extends net.openhft.koloboke.collect.ObjCollection<O> {
    void reclaim();
}
