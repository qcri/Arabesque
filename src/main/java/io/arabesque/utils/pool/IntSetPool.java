package io.arabesque.utils.pool;

import io.arabesque.utils.BasicFactory;
import io.arabesque.utils.Factory;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

public class IntSetPool extends CollectionPool<IntSet> {
    private static final Factory<IntSet> factory = new BasicFactory<IntSet>() {
        @Override
        public IntSet createObject() {
            return HashIntSets.newMutableSet();
        }
    };

    public static IntSetPool instance() {
        return IntSetPoolHolder.INSTANCE;
    }

    public IntSetPool() {
        super(factory);
    }

    /*
     * Delayed creation of IntSetPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class IntSetPoolHolder {
        static final IntSetPool INSTANCE = new IntSetPool();
    }
}
