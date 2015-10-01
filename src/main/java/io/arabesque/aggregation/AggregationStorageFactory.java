package io.arabesque.aggregation;

import io.arabesque.conf.Configuration;
import io.arabesque.pattern.Pattern;

public class AggregationStorageFactory {

    public AggregationStorage createAggregationStorage(String name) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        return createAggregationStorage(name, metadata);
    }

    public AggregationStorage createAggregationStorage(String name, AggregationStorageMetadata metadata) {
        if (metadata == null) {
            throw new RuntimeException("Attempted to create unregistered aggregation storage");
        }

        Class<?> keyClass = metadata.getKeyClass();

        if (Pattern.class.isAssignableFrom(keyClass)) {
            return new PatternAggregationStorage(name);
        } else {
            return new AggregationStorage(name);
        }
    }
}
