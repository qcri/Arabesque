package io.arabesque.gmlib.fsm;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.MasterComputation;
import io.arabesque.pattern.Pattern;

public class FSMMasterComputation extends MasterComputation {
    @Override
    public void compute() {
        System.out.println ("Master computing");
        AggregationStorage<Pattern, DomainSupport> aggregationStorage =
                readAggregation(FSMComputation.AGG_SUPPORT);
            
        System.out.println("Aggregation Storage: " + aggregationStorage);

        if (aggregationStorage.getNumberMappings() > 0) {
            System.out.println("Frequent patterns:");

            int i = 1;
            for (Pattern pattern : aggregationStorage.getKeys()) {
                System.out.println(i + ": " + pattern + ": " + aggregationStorage.getValue(pattern));
                ++i;
            }
        }
        // If frequent patterns is empty and superstep > 0, halt
        else if (getStep() > 0) {
            haltComputation();
        }
    }
}

