package io.arabesque.optimization;

import io.arabesque.graph.MainGraph;
import io.arabesque.utils.collection.IntArrayList;

public class BiggerNeighboursMainGraphDecorator extends OrderedNeighboursMainGraphDecorator {

    public BiggerNeighboursMainGraphDecorator(MainGraph underlyingMainGraph) {
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
