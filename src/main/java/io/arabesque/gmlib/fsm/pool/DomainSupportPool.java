package io.arabesque.gmlib.fsm.pool;

import io.arabesque.conf.Configuration;
import io.arabesque.gmlib.fsm.DomainSupport;
import io.arabesque.utils.BasicFactory;
import io.arabesque.utils.Factory;
import io.arabesque.utils.pool.Pool;

public class DomainSupportPool extends Pool<DomainSupport> {

    public static DomainSupportPool instance() {
        return DomainSupportPoolHolder.INSTANCE;
    }

    public DomainSupportPool(Factory<DomainSupport> factory) {
        super(factory);
    }

    public DomainSupport createObject(int support) {
       DomainSupport domainSupport = this.createObject();
       domainSupport.setSupport (support);
       return domainSupport;
    }

    private static class DomainSupportFactory extends BasicFactory<DomainSupport> {
        @Override
        public DomainSupport createObject() {
           return new DomainSupport();
        }
    }

    /*
     * Delayed creation of DomainSupportPool. instance will only be instantiated when we call
     * the static method instance().
     *
     * This initialization is also guaranteed to be thread-safe.
     */
    private static class DomainSupportPoolHolder {
        static final DomainSupportPool INSTANCE = new DomainSupportPool(new DomainSupportFactory());
    }
}
