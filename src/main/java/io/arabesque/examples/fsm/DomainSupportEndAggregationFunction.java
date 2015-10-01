package io.arabesque.examples.fsm;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.EndAggregationFunction;
import io.arabesque.pattern.Pattern;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DomainSupportEndAggregationFunction
        implements EndAggregationFunction<Pattern, DomainSupport> {
    @Override
    public void endAggregation(AggregationStorage<Pattern, DomainSupport> aggregationStorage) {
        Set<Pattern> patternsToRemove = new HashSet<>();

        Map<Pattern, DomainSupport> finalMapping = aggregationStorage.getMapping();

        for (Map.Entry<Pattern, DomainSupport> finalMappingEntry : finalMapping.entrySet()) {
            Pattern pattern = finalMappingEntry.getKey();
            DomainSupport domainSupport = finalMappingEntry.getValue();

            if (!domainSupport.hasEnoughSupport()) {
                patternsToRemove.add(pattern);
            }
        }

        aggregationStorage.removeKeys(patternsToRemove);
    }
}
