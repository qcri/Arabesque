package io.arabesque.graph;

import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionMultiplexer;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.map.IntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.function.IntConsumer;
import net.openhft.koloboke.function.IntFunction;

public class MultiVertexNeighbourhood extends BasicVertexNeighbourhood {
    private static final IntFunction<IntArrayList> INTARRAYLIST_FACTORY = new IntFunction<IntArrayList>() {
        @Override
        public IntArrayList apply(int i) {
            // Prevent reclaiming of these array lists as they might be returned by
            // getEdgesWithNeighbourVertex but should survive the entire execution.
            return new IntArrayList(true);
        }
    };

    // Map used to store connections through multiple edges. Single edge collections use the
    // underlying neighbourhoodMap
    // Key = neighbour vertex id, Value = array with edge ids that connects owner of neighbourhood with Key
    private IntObjMap<IntArrayList> multiEdgeNeighbourhoodMap;
    private IntCollectionMultiplexer neighbourEdges;
    private IntCollectionMultiplexer neighbourVertices;

    public MultiVertexNeighbourhood() {
        this.multiEdgeNeighbourhoodMap = HashIntObjMaps.newMutableMap();
        this.neighbourEdges = null;
        this.neighbourVertices = null;
    }

    @Override
    public IntCollection getNeighbourVertices() {
        if (neighbourVertices == null) {
            neighbourVertices = new IntCollectionMultiplexer();
            neighbourVertices.addCollection(neighbourhoodMap.keySet());
            neighbourVertices.addCollections(multiEdgeNeighbourhoodMap.keySet());
        }

        return neighbourVertices;
    }

    @Override
    public IntCollection getNeighbourEdges() {
        if (neighbourEdges == null) {
            neighbourEdges = new IntCollectionMultiplexer();
            neighbourEdges.addCollection(neighbourhoodMap.values());
            neighbourEdges.addCollections(multiEdgeNeighbourhoodMap.values());
        }

        return neighbourEdges;
    }

    @Override
    public ReclaimableIntCollection getEdgesWithNeighbourVertex(int neighbourVertexId) {
        ReclaimableIntCollection edgeIds = multiEdgeNeighbourhoodMap.get(neighbourVertexId);

        if (edgeIds != null) {
            // These should be protected against reclaiming because they must survive
            // during the entire execution.
            return edgeIds;
        }

        return super.getEdgesWithNeighbourVertex(neighbourVertexId);
    }

    @Override
    public void forEachEdgeId(int nId, IntConsumer intConsumer) {
        ReclaimableIntCollection edgeIds = multiEdgeNeighbourhoodMap.get(nId);

        if (edgeIds != null) {
            edgeIds.forEach(intConsumer);
        }

        super.forEachEdgeId(nId, intConsumer);
    }

    @Override
    public boolean isNeighbourVertex(int vertexId) {
        return super.isNeighbourVertex(vertexId) || multiEdgeNeighbourhoodMap.containsKey(vertexId);
    }

    @Override
    public void addEdge(int neighbourVertexId, int edgeId) {
        boolean singleMapHasNeighbourId = neighbourhoodMap.containsKey(neighbourVertexId);
        // If this is the first edge between these 2 vertices, add it to the underlying map
        if (!singleMapHasNeighbourId && !multiEdgeNeighbourhoodMap.containsKey(neighbourVertexId)) {
            super.addEdge(neighbourVertexId, edgeId);
        }
        // If this is the 2+ edge between these 2 vertices,
        else {
            IntCollection edgeList = multiEdgeNeighbourhoodMap.computeIfAbsent(neighbourVertexId, INTARRAYLIST_FACTORY);

            if (singleMapHasNeighbourId) {
                int existingEdgeId = neighbourhoodMap.remove(neighbourVertexId);
                edgeList.add(existingEdgeId);
            }

            edgeList.add(edgeId);
        }
    }

    @Override
    public String toString() {
        return "MultiVertexNeighbourhood{" +
                "multiEdgeNeighbourhoodMap=" + multiEdgeNeighbourhoodMap +
                ", neighbourEdges=" + neighbourEdges +
                "} " + super.toString();
    }
}
