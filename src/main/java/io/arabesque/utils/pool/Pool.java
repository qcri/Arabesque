package io.arabesque.utils.pool;

import io.arabesque.utils.Factory;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.collection.ReclaimableObjCollection;
import net.openhft.koloboke.function.Consumer;

public class Pool<O> {
    private final static int MAX_SIZE_DEFAULT = 1000;

    private int maxSize;
    private Factory<O> objectFactory;
    private PoolStorage poolStorage;
    private final ObjReclaimerStorage reclaimerStorage;

    public Pool(Factory<O> objectFactory) {
        this(objectFactory, MAX_SIZE_DEFAULT);
    }

    public Pool(Factory<O> objectFactory, int maxSize) {
        this.objectFactory = objectFactory;
        this.maxSize = maxSize;
        reclaimerStorage = new ObjReclaimerStorage();

        reset();

        PoolRegistry.instance().register(this.getClass().getSimpleName(), this);
    }

    public void reset() {
        objectFactory.reset();
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
        reclaimerStorage.get().accept(object);
    }

    public void reclaimObjects(ReclaimableObjCollection<O> objects) {
        objects.forEach(reclaimerStorage.get());
    }

    protected ObjReclaimer createObjReclaimer() {
        return new ObjReclaimer();
    }

    protected class ObjReclaimer implements Consumer<O> {
        private ObjArrayList<O> pool;

        public ObjReclaimer() {
            pool = poolStorage.get();
        }

        @Override
        public void accept(O o) {
            if (pool.size() < maxSize) {
                pool.add(o);
            }
        }
    }

    private class PoolStorage extends ThreadLocal<ObjArrayList<O>> {
        @Override
        protected ObjArrayList<O> initialValue() {
            return new ObjArrayList<>(maxSize);
        }
    }

    private class ObjReclaimerStorage extends ThreadLocal<ObjReclaimer> {
        @Override
        protected ObjReclaimer initialValue() {
            return createObjReclaimer();
        }
    }
}
