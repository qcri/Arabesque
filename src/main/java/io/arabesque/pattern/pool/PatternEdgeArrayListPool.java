package io.arabesque.pattern.pool;

import io.arabesque.pattern.PatternEdgeArrayList;
import io.arabesque.utils.BasicFactory;
import io.arabesque.utils.Factory;
import io.arabesque.utils.pool.CollectionPool;

public class PatternEdgeArrayListPool extends CollectionPool<PatternEdgeArrayList> {
    private static final Factory<PatternEdgeArrayList> factory = new BasicFactory<PatternEdgeArrayList>() {
        @Override
        public PatternEdgeArrayList createObject() {
            return new PatternEdgeArrayList();
        }
    };

    public static PatternEdgeArrayListPool instance() {
        return PatternEdgeArrayListPoolHolder.INSTANCE;
    }

    public PatternEdgeArrayListPool() {
        super(factory);
    }

    @Override
    public void reclaimObject(PatternEdgeArrayList object) {
        PatternEdgePool.instance().reclaimObjects(object);
        super.reclaimObject(object);
    }

    /*
     * Delayed creation of IntArrayListPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class PatternEdgeArrayListPoolHolder {
        static final PatternEdgeArrayListPool INSTANCE = new PatternEdgeArrayListPool();
    }
}
