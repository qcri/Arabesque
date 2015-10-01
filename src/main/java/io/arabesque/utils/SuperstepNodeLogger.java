package io.arabesque.utils;

import org.apache.giraph.worker.DefaultWorkerObserver;

public class SuperstepNodeLogger extends DefaultWorkerObserver {
    private int getNodeId() {
        return getConf().getTaskPartition();
    }

    @Override
    public void preSuperstep(long superstep) {
        System.out.println("#S " + "@TASK " + getNodeId() + " @SUPERSTEP " + superstep + " TIME " + System.currentTimeMillis());
    }

    @Override
    public void postSuperstep(long superstep) {
        System.out.println("#F " + "@TASK " + getNodeId() + " @SUPERSTEP " + superstep + " TIME " + System.currentTimeMillis());
    }
}