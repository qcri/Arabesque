package io.arabesque.utils;

import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.function.IntConsumer;

public class ClearSetConsumer implements IntConsumer {
    private HashIntSet[] supportMatrix;

    public void setSupportMatrix(HashIntSet[] supportMatrix) {
        this.supportMatrix = supportMatrix;
    }

    @Override
    public void accept(int i) {
        HashIntSet domainSet = supportMatrix[i];

        if (domainSet != null) {
            domainSet.clear();
        }
    }
}
