package io.arabesque.graph;

import io.arabesque.utils.collection.ReclaimableIntCollection;
import io.arabesque.utils.pool.IntSingletonPool;
import com.koloboke.collect.IntCollection;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.function.IntConsumer;

public class BasicVertexNeighbourhood implements VertexNeighbourhood, java.io.Serializable {
    // Key = neighbour vertex id, Value = edge id that connects owner of neighbourhood with Key
    protected IntIntMap neighbourhoodMap;

    public BasicVertexNeighbourhood() {
        this.neighbourhoodMap = HashIntIntMaps.getDefaultFactory().withDefaultValue(-1).newMutableMap();
    }

    @Override
    public IntCollection getNeighbourVertices() {
        return neighbourhoodMap.keySet();
    }

    @Override
    public IntCollection getNeighbourEdges() {
        return neighbourhoodMap.values();
    }

    public int getEdgeWithNeighbourVertex(int neighbourVertexId) {
        return neighbourhoodMap.get(neighbourVertexId);
    }

    @Override
    public ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId) {
        int edgeId = neighbourhoodMap.get(neighbourVertexId);

        if (edgeId >= 0) {
            return IntSingletonPool.instance().createObject(edgeId);
        }
        else {
            return null;
        }
    }

    @Override
    public void forEachEdgeId(int nId, IntConsumer intConsumer) {
        int edgeId = neighbourhoodMap.get(nId);

        if (edgeId >= 0) {
            intConsumer.accept(edgeId);
        }
    }

    @Override
    public boolean isNeighbourVertex(int vertexId) {
        return neighbourhoodMap.containsKey(vertexId);
    }

    @Override
    public void addEdge(int neighbourVertexId, int edgeId) {
        neighbourhoodMap.put(neighbourVertexId, edgeId);
    }

    @Override
    public String toString() {
        return "BasicVertexNeighbourhood{" +
                "neighbourhoodMap=" + neighbourhoodMap +
                '}';
    }
}
