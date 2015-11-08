package io.arabesque.utils.pool;

import io.arabesque.utils.Factory;

import java.util.Collection;

public class CollectionPool<O extends Collection> extends Pool<O> {
    public CollectionPool(Factory<O> objectFactory) {
        super(objectFactory);
    }

    public CollectionPool(Factory<O> objectFactory, int maxSize) {
        super(objectFactory, maxSize);
    }

    @Override
    public void reclaimObject(O object) {
        object.clear();
        super.reclaimObject(object);
    }
}
