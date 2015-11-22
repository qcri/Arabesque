package io.arabesque.utils.pool;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PoolRegistry {
    private Map<String, Pool> poolMap;

    public PoolRegistry() {
        this.poolMap = new HashMap<>();
    }

    public synchronized void register(String poolId, Pool pool) {
        poolMap.put(poolId, pool);
    }

    public synchronized Map<String, Pool> getPoolMap() {
        return poolMap;
    }

    public synchronized Collection<Pool> getPools() {
        return poolMap.values();
    }

    public static PoolRegistry instance() {
        return PoolRegistryHolder.INSTANCE;
    }

    /*
     * Delayed creation of PoolRegistry. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class PoolRegistryHolder {
        static final PoolRegistry INSTANCE = new PoolRegistry();
    }
}
