package io.arabesque.gmlib.disconnectedGraphFSM;

import io.arabesque.aggregation.reductions.ReductionFunction;

public class DisconnectedGraphSupportReducer
   extends ReductionFunction<DisconnectedGraphSupport> {
    @Override
    public DisconnectedGraphSupport reduce(DisconnectedGraphSupport k1, DisconnectedGraphSupport k2) {
        k1.aggregate(k2);

        return k1;
    }
}
