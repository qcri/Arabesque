package io.arabesque.examples.all_pairs;

import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.UnsafeCSRMainGraphEdgeLabelFloat;

/**
 * Created by siganos on 2/16/16.
 */
public class AllPairsComputation extends VertexInducedComputation<VertexInducedEmbedding> {
    private BellmanFord bellmanFord;



    @Override
    public void init() {
      super.init();
      bellmanFord = new BellmanFord(getMainGraph().getNumberVertices());
    }

    @Override
    // we do all the work in filter to avoid the overhead of accessing the embedding...
    public boolean filter(VertexInducedEmbedding newEmbedding) {
        int source  = newEmbedding.getWords().get(0);
        //We know that it is float the label.
        bellmanFord.relax((UnsafeCSRMainGraphEdgeLabelFloat) getMainGraph(),source);
        output(bellmanFord.print_array(source));
        return false;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        //Nothing always filter fails...
    }


}
