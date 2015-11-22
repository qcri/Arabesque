package io.arabesque.utils.pool;

import io.arabesque.utils.BasicFactory;
import io.arabesque.utils.Factory;
import io.arabesque.utils.collection.IntArrayList;

public class IntArrayListPool extends CollectionPool<IntArrayList> {
    private static final Factory<IntArrayList> factory = new BasicFactory<IntArrayList>() {
        @Override
        public IntArrayList createObject() {
            return new IntArrayList();
        }
    };

    public static IntArrayListPool instance() {
        return IntArrayListPoolHolder.INSTANCE;
    }

    public IntArrayListPool() {
        super(factory);
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class IntArrayListPoolHolder {
        static final IntArrayListPool INSTANCE = new IntArrayListPool();
    }
}
