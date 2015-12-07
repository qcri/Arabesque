package io.arabesque.utils.collection;

import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.function.IntIntConsumer;

public class IntIntMapAddConsumer implements IntIntConsumer {
    private IntIntMap map;

    public void setMap(IntIntMap map) {
        this.map = map;
    }

    @Override
    public void accept(int k, int v) {
        map.put(k, v);
    }
}
