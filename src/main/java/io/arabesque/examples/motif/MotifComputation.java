package io.arabesque.examples.motif;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import org.apache.hadoop.io.LongWritable;

public class MotifComputation extends VertexInducedComputation<VertexInducedEmbedding> {
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final int MAXSIZE_DEFAULT = 4;

    private static LongWritable reusableLongWritableUnit = new LongWritable(1);

    private int maxsize;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
    }

    @Override
    public void initAggregations() {
        super.initAggregations();

        Configuration conf = Configuration.get();

        conf.registerOutputAggregation(conf.getPatternClass(), LongWritable.class, new LongSumReduction());
    }

    @Override
    public boolean shouldExpand(VertexInducedEmbedding embedding) {
        return embedding.getNumVertices() < maxsize;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        output(embedding);
        mapOutput(embedding.getPattern(), reusableLongWritableUnit);
    }
}
