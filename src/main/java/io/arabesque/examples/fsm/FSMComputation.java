package io.arabesque.examples.fsm;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.EdgeInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.pattern.Pattern;

public class FSMComputation extends EdgeInducedComputation<EdgeInducedEmbedding> {
    public static final String CONF_SUPPORT = "arabesque.fsm.support";
    public static final int CONF_SUPPORT_DEFAULT = 4;

    public static final String CONF_MAXSIZE = "arabesque.fsm.maxsize";
    public static final int CONF_MAXSIZE_DEFAULT = -1;

    private DomainSupport reusableDomainSupport;

    private AggregationStorage<Pattern, DomainSupport> previousStepAggregation;

    private int maxSize;
    private int support;

    @Override
    public void init() {
        super.init();

        Configuration conf = Configuration.get();

        support = conf.getInteger(CONF_SUPPORT, CONF_SUPPORT_DEFAULT);
        maxSize = conf.getInteger(CONF_MAXSIZE, CONF_MAXSIZE_DEFAULT);

        reusableDomainSupport = new DomainSupport(support);

        previousStepAggregation = readAggregation("support");
    }

    @Override
    public void initAggregations() {
        super.initAggregations();

        Configuration conf = Configuration.get();

        conf.registerAggregation("support", conf.getPatternClass(), DomainSupport.class, true,
                new DomainSupportReducer(), new DomainSupportEndAggregationFunction());
    }

    @Override
    public boolean shouldExpand(EdgeInducedEmbedding embedding) {
        return maxSize < 0 || embedding.getNumWords() < maxSize;
    }

    @Override
    public void process(EdgeInducedEmbedding embedding) {
        reusableDomainSupport.setFromEmbedding(embedding);
        map("support", embedding.getPattern(), reusableDomainSupport);
    }

    @Override
    public boolean aggregationFilter(EdgeInducedEmbedding embedding) {
        // Using the DomainSupportEndAggregationFunction, we removed all mappings for
        // non-frequent patterns. So we simply have to check if the mapping has
        // the pattern for the corresponding key
        return previousStepAggregation.containsKey(embedding.getPattern());
    }

    @Override
    public void aggregationProcess(EdgeInducedEmbedding embedding) {
        output(embedding);
    }
}
