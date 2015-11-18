package io.arabesque.utils.collection;

import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.function.IntConsumer;

public class IntCollectionAddConsumer implements IntConsumer {
    private IntCollection collection;

    public void setCollection(IntCollection collection) {
        this.collection = collection;
    }

    @Override
    public void accept(int i) {
        collection.add(i);
    }
}
