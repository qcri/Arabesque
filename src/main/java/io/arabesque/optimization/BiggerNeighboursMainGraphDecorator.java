package io.arabesque.optimization;

import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.utils.IntArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class BiggerNeighboursMainGraphDecorator<
        VD extends Writable,
        QV extends Vertex<VD>,
        EL extends WritableComparable,
        QE extends Edge<EL>> extends OrderedNeighboursMainGraphDecorator<VD, QV, EL, QE> {

    public BiggerNeighboursMainGraphDecorator(MainGraph<VD, QV, EL, QE> underlyingMainGraph) {
        super(underlyingMainGraph);

        for (int vertexId = 0; vertexId < orderedNeighbours.length; ++vertexId) {
            IntArrayList orderedNeighboursOfVertex = orderedNeighbours[vertexId];

            if (orderedNeighboursOfVertex == null) {
                continue;
            }

            orderedNeighboursOfVertex.removeSmaller(vertexId);
        }
    }
}
