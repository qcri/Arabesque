package io.arabesque.examples.fsm;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.EdgeInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.EdgeInducedEmbedding;
import io.arabesque.pattern.Pattern;
import org.apache.log4j.Logger;

public class FSMComputation extends EdgeInducedComputation<EdgeInducedEmbedding> {
    private static final Logger LOG = Logger.getLogger(FSMComputation.class);
    public static final String AGG_SUPPORT = "support";

    public static final String CONF_SUPPORT = "arabesque.fsm.support";
    public static final int CONF_SUPPORT_DEFAULT = 4;

    public static final String CONF_MAXSIZE = "arabesque.fsm.maxsize";
    public static final int CONF_MAXSIZE_DEFAULT = Integer.MAX_VALUE;

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

        previousStepAggregation = readAggregation(AGG_SUPPORT);
    }

    @Override
    public void initAggregations() {
        super.initAggregations();

        Configuration conf = Configuration.get();

        conf.registerAggregation(AGG_SUPPORT, conf.getPatternClass(), DomainSupport.class, false,
                new DomainSupportReducer(), new DomainSupportEndAggregationFunction());
    }
    
    @Override
    public boolean shouldExpand(EdgeInducedEmbedding embedding) {
        return embedding.getNumWords() < maxSize;
    }

    @Override
    public void process(EdgeInducedEmbedding embedding) {
        reusableDomainSupport.setFromEmbedding(embedding);
        map(AGG_SUPPORT, embedding.getPattern(), reusableDomainSupport);
    }

    @Override
    public boolean aggregationFilter(Pattern pattern) {
        return previousStepAggregation.containsKey(pattern);
    }

    @Override
    public void aggregationProcess(EdgeInducedEmbedding embedding) {
        output(embedding);
    }
}
