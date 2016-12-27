package io.arabesque.utils.collection;

import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;

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
