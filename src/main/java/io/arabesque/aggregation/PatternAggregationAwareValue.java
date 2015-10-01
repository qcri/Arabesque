package io.arabesque.aggregation;

import io.arabesque.pattern.Pattern;

public interface PatternAggregationAwareValue {
    public void handleConversionFromQuickToCanonical(Pattern quickPattern, Pattern canonicalPattern);
}
