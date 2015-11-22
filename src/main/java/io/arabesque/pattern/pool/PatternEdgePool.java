package io.arabesque.pattern.pool;

import io.arabesque.conf.Configuration;
import io.arabesque.pattern.LabelledPatternEdge;
import io.arabesque.pattern.PatternEdge;
import io.arabesque.utils.BasicFactory;
import io.arabesque.utils.Factory;
import io.arabesque.utils.pool.Pool;

public class PatternEdgePool extends Pool<PatternEdge> {

    public static PatternEdgePool instance() {
        return PatternEdgePoolHolder.INSTANCE;
    }

    public PatternEdgePool(Factory<PatternEdge> factory) {
        super(factory);
    }

    private static class PatternEdgeFactory extends BasicFactory<PatternEdge> {
        private boolean areEdgesLabelled;

        @Override
        public PatternEdge createObject() {
            if (!areEdgesLabelled) {
                return new PatternEdge();
            }
            else {
                return new LabelledPatternEdge();
            }
        }

        @Override
        public void reset() {
            Configuration conf = Configuration.get();
            areEdgesLabelled = conf.isGraphEdgeLabelled();
        }
    }

    /*
     * Delayed creation of PatternEdgePool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class PatternEdgePoolHolder {
        static final PatternEdgePool INSTANCE = new PatternEdgePool(new PatternEdgeFactory());
    }
}
