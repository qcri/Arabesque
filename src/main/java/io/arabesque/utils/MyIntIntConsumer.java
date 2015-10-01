package io.arabesque.utils;

import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.function.IntIntConsumer;

public class MyIntIntConsumer implements IntIntConsumer {
    HashIntIntMap map;

    public void setMap(HashIntIntMap map) {
        this.map = map;
    }

    @Override
    public void accept(int i, int i1) {
        map.addValue(i, 1);
    }
}
