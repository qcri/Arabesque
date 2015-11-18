package io.arabesque.utils.collection;

import net.openhft.koloboke.collect.IntCollection;

public interface ReclaimableIntCollection extends IntCollection {
    void reclaim();
}
