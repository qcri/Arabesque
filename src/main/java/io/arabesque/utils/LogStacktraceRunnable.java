package io.arabesque.utils;

import org.apache.giraph.utils.LogStacktraceCallable;
import org.apache.log4j.Logger;

public class LogStacktraceRunnable implements Runnable {
    private static final Logger LOG = Logger.getLogger(LogStacktraceCallable.class);

    private Runnable runnable;

    public LogStacktraceRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try {
            runnable.run();
        } catch (Exception e) {
            LOG.error("Error running runnable.", e);
            throw e;
        }
    }
}

