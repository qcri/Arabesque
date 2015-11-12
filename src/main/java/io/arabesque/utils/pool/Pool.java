package io.arabesque.utils.pool;

import io.arabesque.utils.Factory;
import io.arabesque.utils.ObjArrayList;

import java.util.Collection;

public class Pool<O> {
    private final static int MAX_SIZE_DEFAULT = 1000;

    private int maxSize;
    private Factory<O> objectFactory;
    private final PoolStorage poolStorage;

    public Pool(Factory<O> objectFactory) {
        this(objectFactory, MAX_SIZE_DEFAULT);
    }

    public Pool(Factory<O> objectFactory, int maxSize) {
        this.objectFactory = objectFactory;
        this.maxSize = maxSize;
        poolStorage = new PoolStorage();
    }

    public O createObject() {
        ObjArrayList<O> pool = poolStorage.get();

        if (!pool.isEmpty()) {
            return pool.pop();
        }
        else {
            return objectFactory.createObject();
        }
    }

    public void reclaimObject(O object) {
        ObjArrayList<O> pool = poolStorage.get();

        reclaimObject(pool, object);
    }

    public void reclaimObjects(Collection<O> objects) {
        ObjArrayList<O> pool = poolStorage.get();

        for (O object : objects) {
            reclaimObject(pool, object);
        }
    }

    private void reclaimObject(ObjArrayList<O> pool, O object) {
        if (pool.size() < maxSize) {
            pool.add(object);
        }
    }

    private class PoolStorage extends ThreadLocal<ObjArrayList<O>> {
        @Override
        protected ObjArrayList<O> initialValue() {
            return new ObjArrayList<>(maxSize);
        }
    }
}
