package io.arabesque.gmlib.disconnectedGraphFSM;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.EndAggregationFunction;
import io.arabesque.pattern.Pattern;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DisconnectedGraphSupportEndAggregationFunction
        implements EndAggregationFunction<Pattern, DisconnectedGraphSupport> {
    @Override
    public void endAggregation(AggregationStorage<Pattern, DisconnectedGraphSupport> aggregationStorage) {
        Set<Pattern> patternsToRemove = new HashSet<>();

        Map<Pattern, DisconnectedGraphSupport> finalMapping = aggregationStorage.getMapping();

        for (Map.Entry<Pattern, DisconnectedGraphSupport> finalMappingEntry : finalMapping.entrySet()) {
            Pattern pattern = finalMappingEntry.getKey();
            DisconnectedGraphSupport domainSupport = finalMappingEntry.getValue();

            if (!domainSupport.hasEnoughSupport()) {
                patternsToRemove.add(pattern);
            }
        }

        aggregationStorage.removeKeys(patternsToRemove);
    }
}
