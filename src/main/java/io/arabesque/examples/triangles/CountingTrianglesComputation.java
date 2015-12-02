package io.arabesque.examples.triangles;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class CountingTrianglesComputation extends VertexInducedComputation<VertexInducedEmbedding> {
    private static final String AGG_OUTPUT = "output";
    private static final LongWritable unitLongWritable = new LongWritable(1);

    private final IntWritable reusableIdWritable = new IntWritable();

    @Override
    public void initAggregations() {
        super.initAggregations();

        Configuration.get().registerAggregation(
        		AGG_OUTPUT,
                IntWritable.class,
                LongWritable.class,
                true,
                new LongSumReduction()
        );
    }

    @Override
    public boolean filter(VertexInducedEmbedding embedding) {
        return embedding.getNumVertices() < 3 ||
                (embedding.getNumVertices() == 3 && embedding.getNumEdges() == 3);
    }

    @Override
    public boolean shouldExpand(VertexInducedEmbedding embedding) {
        return embedding.getNumVertices() < 3;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        if (embedding.getNumVertices() == 3) {
            IntArrayList vertices = embedding.getVertices();

            for (int i = 0; i < 3; ++i) {
                reusableIdWritable.set(vertices.getUnchecked(i));
                map(AGG_OUTPUT, reusableIdWritable, unitLongWritable);
            }
        }
    }
}
