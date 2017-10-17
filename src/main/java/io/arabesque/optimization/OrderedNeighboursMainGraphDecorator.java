package io.arabesque.optimization;

import io.arabesque.graph.BasicMainGraph;
import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.ReclaimableIntCollection;
import com.koloboke.collect.IntCollection;
import com.koloboke.function.IntConsumer;

public class OrderedNeighboursMainGraphDecorator implements OrderedNeighboursMainGraph {
    protected MainGraph underlyingMainGraph;

    protected IntArrayList[] orderedNeighbours;

    public OrderedNeighboursMainGraphDecorator(MainGraph underlyingMainGraph) {
        this.underlyingMainGraph = underlyingMainGraph;

        int numVertices = underlyingMainGraph.getNumberVertices();

        orderedNeighbours = new IntArrayList[numVertices];
        BasicMainGraph g_ = (BasicMainGraph) underlyingMainGraph;

        for (int i = 0; i < numVertices; ++i) {
            IntCollection neighboursOfI = g_.getVertexNeighbors(i);//getVertexNeighbours(i);

            if (neighboursOfI != null) {
                orderedNeighbours[i] = new IntArrayList(neighboursOfI);
                orderedNeighbours[i].sort();
            }
        }
    }

    @Override
    public void reset() {
        underlyingMainGraph.reset();
    }

    @Override
    public int getVertexLabel(int v) {
        return underlyingMainGraph.getVertexLabel(v);
    }

    @Override
    public boolean isNeighborVertex(int v1, int v2) {
        return underlyingMainGraph.isNeighborVertex(v1, v2);
    }

    @Override
    public MainGraph addVertex(Vertex vertex) {
        return underlyingMainGraph.addVertex(vertex);
    }

    @Override
    public int getNumberVertices() {
        return underlyingMainGraph.getNumberVertices();
    }

    @Override
    public int getNumberEdges() {
        return underlyingMainGraph.getNumberEdges();
    }

    @Override
    public int getEdgeLabel(int edgeId) {
        return underlyingMainGraph.getEdgeLabel(edgeId);
    }

    @Override
    public int getEdgeSource(int edgeId) {
        return underlyingMainGraph.getEdgeSource(edgeId);
    }

    @Override
    public int getEdgeDst(int edgeId) {
        return underlyingMainGraph.getEdgeDst(edgeId);
    }

    @Override
    public int neighborhoodSize(int vertexId) {
        return underlyingMainGraph.neighborhoodSize(vertexId);
    }

    @Override
    public MainGraph addEdge(Edge edge) {
        return underlyingMainGraph.addEdge(edge);
    }

    @Override
    public boolean areEdgesNeighbors(int edge1Id, int edge2Id) {
        return underlyingMainGraph.areEdgesNeighbors(edge1Id, edge2Id);
    }

//    @Override
//    public boolean isNeighborEdge(int src1, int dest1, int edge2) {
//        return underlyingMainGraph.isNeighborEdge(src1, dest1, edge2);
//    }

    @Override
    public void processVertexNeighbors(int vertexId, IntConsumer intAddConsumer) {
        underlyingMainGraph.processVertexNeighbors(vertexId, intAddConsumer);
    }

    @Override
    public void processEdgeNeighbors(int vertexId, IntConsumer intAddConsumer) {
        underlyingMainGraph.processEdgeNeighbors(vertexId, intAddConsumer);
    }

    @Override
    public boolean isEdgeLabelled() {
        return underlyingMainGraph.isEdgeLabelled();
    }

    @Override
    public boolean isMultiGraph() {
        return underlyingMainGraph.isMultiGraph();
    }

    @Override
    public void forEachEdgeId(int existingVertexId, int newVertexId, IntConsumer intConsumer) {
        underlyingMainGraph.forEachEdgeId(existingVertexId, newVertexId, intConsumer);
    }
}
