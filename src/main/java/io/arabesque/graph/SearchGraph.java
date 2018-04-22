package io.arabesque.graph;

import io.arabesque.utils.collection.IntArrayList;
import com.koloboke.collect.IntIterator;

/**
 * Used for the search function where we require a reverse lookup from label to vertex.
 * Created by siganos on 3/21/16.
 */
public interface SearchGraph extends MainGraph {

    int getNeighborhoodSizeWithLabel(int i, int label);
    long getNumberVerticesWithLabel(int label);
//    int getNumberEdgesWithLabel(int label);

    boolean hasEdgesWithLabels(int source, int destination, IntArrayList edgeLabels);

    void setIteratorForNeighborsWithLabel(int vertexId, int vertexLabel, IntIterator one);
    IntArrayList getVerticesWithLabel(int vertexLabel);

    IntIterator createNeighborhoodSearchIterator();
    boolean isNeighborVertexWithLabel(int sourceVertexId, int destVertexId, int destinationLabel);

}
