package io.arabesque.gmlib.disconnectedGraphFSM;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.MasterComputation;
import io.arabesque.pattern.Pattern;

/*
In a Disconnected Graph, the FSM algorithm counts the frequency based on the occurences across subgraphs.
i.e. The frequency is the number of subgraphs where a pattern exists
 */

public class DisconnectedGraphFSMMasterComputation extends MasterComputation {
    @Override
    public void compute() {
        AggregationStorage<Pattern, DisconnectedGraphSupport> aggregationStorage =
                readAggregation(DisconnectedGraphFSMComputation.AGG_SUPPORT);

        System.out.println("Aggregation Storage: " + aggregationStorage);

        if (aggregationStorage.getNumberMappings() > 0) {
            System.out.println("Frequent patterns count: " + aggregationStorage.getNumberMappings());

//            int i = 1;
//            for (Pattern pattern : aggregationStorage.getKeys()) {
//                System.out.println(i + ": " + pattern + ": " + aggregationStorage.getValue(pattern));
//                ++i;
//            }
        }
        // If frequent patterns is empty and superstep > 0, halt
        else if (getStep() > 0) {
            haltComputation();
        }
    }
}

