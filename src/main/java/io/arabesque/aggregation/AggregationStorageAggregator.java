package io.arabesque.aggregation;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

public class AggregationStorageAggregator extends BasicAggregator<AggregationStorageWrapper> {
    @Override
    public void aggregate(AggregationStorageWrapper aggregationStore) {
        AggregationStorageWrapper existing = getAggregatedValue();

        existing.aggregate(aggregationStore);
    }

    @Override
    public AggregationStorageWrapper createInitialValue() {
        return new AggregationStorageWrapper();
    }

    @Override
    public void postSuperstep(String aggregatorKey, ImmutableClassesGiraphConfiguration configuration) {
        super.postSuperstep(aggregatorKey, configuration);

        AggregationStorageWrapper existing = getAggregatedValue();
        existing.endedAggregation();
    }
}
