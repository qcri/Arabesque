package io.arabesque.utils.collection;

import com.koloboke.collect.IntCollection;

public interface ReclaimableIntCollection extends IntCollection {
    void reclaim();
}
