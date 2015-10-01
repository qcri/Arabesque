package io.arabesque.utils;

import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.function.IntIntConsumer;

public class SetIntValueConsumer implements IntIntConsumer {
    HashIntSet set;

    public void setSet(HashIntSet set) {
        this.set = set;
    }

    @Override
    public void accept(int i, int i1) {
        set.add(i1);
    }
}
