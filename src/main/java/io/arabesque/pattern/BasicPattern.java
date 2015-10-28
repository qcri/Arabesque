package io.arabesque.pattern;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BasicPattern extends Pattern {
    private static final int MAX_INT = 2147483647;

    private HashIntIntMap adjListMap;
    private int[][] adjListDst;
    private int[] adjListLabels;


    int numberOfVertices;
    int numberOfVerticesMinCode;
    int numberOfEdges;
    int numberOfEdgesMinCode;

    int[] vertexMap;
    int[] vertexMapMinCode;
    PatternEdge[] edges;
    PatternEdge[] edgesMinCode;

    MainGraph<?, ?, ?, ?> g;

    HashIntSet[] autoVertexSet;

    public HashIntSet[] getAutoVertexSet() {
        return autoVertexSet;
    }

    public void convertAutoVertexSet(int originalVeterxMap[], int numberOfVertices) {
        HashIntSet[] newAutoVertexSet = new HashIntSet[numberOfVertices];
        for (int i = 0; i < numberOfVertices; i++) {
            newAutoVertexSet[i] = HashIntSets.newMutableSet();
            IntCursor cursor = autoVertexSet[i].cursor();
            while (cursor.moveNext()) {
                int idx = getIndexMap(originalVeterxMap, numberOfVertices, cursor.elem());
                newAutoVertexSet[i].add(idx);
                if (idx == -1)
                    System.err.println("vertex idx map not found!");
            }
        }
        autoVertexSet = newAutoVertexSet;
    }

    public BasicPattern() {
        this.g = Configuration.get().getMainGraph();
        edges = null;
        vertexMap = null;
        numberOfVertices = 0;
        numberOfEdges = 0;

        vertexMapMinCode = null;
        edgesMinCode = null;
        numberOfEdgesMinCode = 0;
        numberOfVerticesMinCode = 0;
    }

    public BasicPattern(int size) {
        this.g = Configuration.get().getMainGraph();

        edges = new PatternEdge[size];
        vertexMap = new int[size + 1];
        numberOfVertices = 0;
        numberOfEdges = 0;

        edgesMinCode = null;
        vertexMapMinCode = null;
        numberOfEdgesMinCode = 0;
        numberOfVerticesMinCode = 0;

    }

    public BasicPattern(BasicPattern other) {
        this.g = Configuration.get().getMainGraph();
        numberOfVertices = other.numberOfVertices;
        numberOfEdges = other.numberOfEdges;

        if (other.edges != null) {
            edges = new PatternEdge[numberOfEdges];

            for (int i = 0; i < numberOfEdges; ++i) {
                PatternEdge otherEdge = other.edges[i];

                if (otherEdge != null) {
                    edges[i] = new PatternEdge(otherEdge);
                } else {
                    edges[i] = null;
                }
            }
        } else {
            edges = null;
        }

        if (other.vertexMap != null) {
            vertexMap = Arrays.copyOf(other.vertexMap, numberOfVertices);
        } else {
            vertexMap = null;
        }

        if (other.autoVertexSet != null) {
            autoVertexSet = new HashIntSet[numberOfVertices];
            for (int i = 0; i < numberOfVertices; i++) {
                autoVertexSet[i] = HashIntSets.newMutableSet();
                IntCursor cursor = other.autoVertexSet[i].cursor();
                while (cursor.moveNext()) {
                    autoVertexSet[i].add(cursor.elem());
                }
            }
        }
    }

    @Override
    public Pattern copy() {
        return new BasicPattern(this);
    }

    @Override
    public void reset() {
        numberOfEdges = 0;
        numberOfVertices = 0;
        numberOfEdgesMinCode = 0;
        numberOfVerticesMinCode = 0;

        edgesMinCode = null;
        vertexMapMinCode = null;
        adjListMap = null;
    }

    @Override
    public void setEmbedding(Embedding embedding) {
        setEmbedding(embedding, true);
    }

    public void setEmbedding(Embedding embedding, boolean useQuickCode) {
        reset();
        int numEdges = embedding.getNumEdges();
        int numVertices = embedding.getNumVertices();

        if (numEdges == 0 && numVertices == 0) {
            return;
        }

        setupStructures(numVertices, numEdges);


        if (numEdges > 0) {
            if (useQuickCode) {
                generateQuickPatternCode(embedding);
            } else {
                computeEmbeddingAdjancyList(embedding);
                generateNaivePatternCode();
            }
        } else {
            if (numVertices > 1) {
                throw new RuntimeException("Embedding with 0 edges and >1 vertices: " + embedding);
            }

            vertexMap[0] = embedding.getVertices()[0];
            ++numVertices;
        }
    }

    public void setupStructures(int numVertices, int numEdges) {
        if (vertexMap == null || vertexMap.length < numVertices) {
            vertexMap = new int[numVertices];
        }

        if (edges == null || edges.length < numEdges) {
            edges = new PatternEdge[numEdges];
        }
    }

    public void generateQuickPatternCode(Embedding embedding) {
        int numEdges = embedding.getNumEdges();
        int[] edges = embedding.getEdges();

        boolean isOk = true;
        for (int i = 0; i < numEdges; ++i) {
            Edge<?> actualWord = g.getEdge(edges[i]);

            int srcId = actualWord.getSourceId();
            int dstId = actualWord.getDestinationId();

            Vertex<?> srcVertex = g.getVertex(srcId);
            Vertex<?> dstVertex = g.getVertex(dstId);

            if (!addEdge(srcVertex.getVertexId(), srcVertex.getVertexLabel(), dstVertex.getVertexId(), dstVertex.getVertexLabel())) {
                Vertex<?> tmp = srcVertex;
                srcVertex = dstVertex;
                dstVertex = tmp;
                if (!addEdge(srcVertex.getVertexId(), srcVertex.getVertexLabel(), dstVertex.getVertexId(), dstVertex.getVertexLabel())) {
                    System.out.println("Problem: " + embedding.toString());
                    isOk = false;
                }
            }
        }

        if (!isOk)
            System.out.println("QuickPatternCode Problem!");
    }

    @Override
    public int getNumberOfVertices() {
        return numberOfVertices;
    }

    public int[] getVertices() {
        return vertexMap;
    }

    @Override
    public PatternEdge[] getPatternEdges() {
        return edges;
    }

    public void generateNaivePatternCode() {

        if (adjListMap == null) {
            computeAdjacencyList();
        }

        int[] extensibleIds = this.getExtensibleVertexIdsDFS();
        //get the edge with smallest id
        if (extensibleIds == null) {

            final int smallestSrcId = smallestSourceIdKey();

            if (smallestSrcId != MAX_INT) {
                //Min source
                int srcLabel = getLabelFromAdjList(smallestSrcId);
                //Min dest
                final int smallestDestId = smallestFromIntArray(adjListDst[adjListMap.get(smallestSrcId)]);
                final int destLabel = getLabelFromAdjList(smallestDestId);

                if (this.addEdge(smallestSrcId, srcLabel, smallestDestId, destLabel)) {
                    generateNaivePatternCode();
                }
            }
        } else {
            PatternEdge minEdge = null;
            int k = 0;
            while (extensibleIds[k] >= 0) {
                int i = extensibleIds[k];
                k++;
                int srcId = i;
                int srcLabel = getLabelFromAdjList(i);
                int[] dests = adjListDst[adjListMap.get(i)];
                int kkpos = 0;
                while (dests[kkpos] >= 0) {
                    int destId = dests[kkpos];
                    kkpos++;
                    int destLabel = getLabelFromAdjList(destId);
                    PatternEdge edge = new PatternEdge(srcId, srcLabel, destId, destLabel);
                    if (this.addEdge(edge)) {
                        if (minEdge == null || edge.isSmaller(minEdge))
                            minEdge = edge;
                        this.removeLastEdge();
                    }
                }
                if (minEdge != null) {
                    break;
                }
            }
            if (minEdge == null) {
                return;
            } else {
                this.addEdge(minEdge);
                generateNaivePatternCode();
            }
        }
    }

    private int smallestSourceIdKey() {
        IntIntCursor cursor = adjListMap.cursor();
        int minKey = MAX_INT;

        while (cursor.moveNext()) {
            final int key = cursor.key();
            if (key < minKey)
                minKey = key;
        }

        return minKey;
    }

    private int smallestFromIntArray(int[] data) {
        int minKey = MAX_INT;

        int kpos = 0;

        while (data[kpos] >= 0) {
            if (data[kpos] < minKey) {
                minKey = data[kpos];
            }
            kpos++;
        }
        return minKey;
    }

    public int getIndexMap(int n) {
        return getIndexMap(vertexMap, numberOfVertices, n);
    }

    public int getIndexMap(int vertexMap[], int numberOfVertices, int n) {
        for (int i = 0; i < numberOfVertices; i++) {
            if (vertexMap[i] == n) {
                return i;
            }
        }
        return -1;
    }

    public boolean addEdge(PatternEdge e) {
        return addEdge(e.getSrcId(), e.getSrcLabel(), e.getDestId(), e.getDestLabel());
    }

    @Override
    public boolean addEdge(int edgeId) {
        Edge edge = g.getEdge(edgeId);

        return addEdge(edge.getSourceId(), edge.getDestinationId());
    }

    public boolean addEdge(int srcId, int destId) {
        Vertex srcVertex = g.getVertex(srcId);
        Vertex dstVertex = g.getVertex(destId);

        return addEdge(srcId, srcVertex.getVertexLabel(), destId, dstVertex.getVertexLabel());
    }

    public boolean addEdge(int srcId, int srcLabel, int destId, int dstLabel) {
        int newSrcId = getIndexMap(srcId);
        int newDestId = getIndexMap(destId);
        boolean type = true;

        if (newSrcId == -1 && newDestId != -1) {
            return false;
        }

        //check if the node was inserted yet
        if (newSrcId != -1 && newDestId != -1) {
            for (int i = 0; i < numberOfEdges; i++) {
                PatternEdge e = edges[i];
                int realESrc = vertexMap[e.getSrcId()];
                int realEDst = vertexMap[e.getDestId()];
                if ((realESrc == srcId && realEDst == destId) ||
                        (realESrc == destId && realEDst == srcId))
                    return false;
            }
            type = false; //bwd
        }

        //add vertex src
        if (newSrcId == -1) {
            this.vertexMap[numberOfVertices] = srcId;
            newSrcId = numberOfVertices;
            numberOfVertices++;
        }

        //add vertex dest
        if (newDestId == -1) {
            this.vertexMap[numberOfVertices] = destId;
            newDestId = numberOfVertices;
            numberOfVertices++;
        }

        //add edge
        if (edges[numberOfEdges] == null)
            edges[numberOfEdges] = new PatternEdge();

        edges[numberOfEdges].setType(type);
        edges[numberOfEdges].setSrcId(newSrcId);
        edges[numberOfEdges].setSrcLabel(srcLabel);
        edges[numberOfEdges].setDestId(newDestId);
        edges[numberOfEdges].setDestLabel(dstLabel);
        numberOfEdges++;

        return true;
    }

    @Override
    public int getNumberOfEdges() {
        return numberOfEdges;
    }

    public void removeLastEdge() {
        if (numberOfEdges <= 1) {
            numberOfEdges = 0;
            numberOfVertices = 0;
            return;
        }
        if (edges[numberOfEdges - 1].getType() == true) {
            numberOfVertices--;
        }
        numberOfEdges--;
    }

    public int[] getExtensibleVertexIdsDFS() {
        if (numberOfEdges == 0)
            return null;

        int[] extensibles = new int[numberOfEdges + 2];//new ArrayList<Integer>();
        int myPos = 0;

        int lastIndex = 0;
        int lastSourceId = 0;

        for (int i = numberOfEdges; i > 0; i--) {
            if (edges[i - 1].getType() == true) {
                lastIndex = i;
                lastSourceId = edges[i - 1].getSrcId();
                extensibles[myPos] = vertexMap[edges[i - 1].getDestId()];
                myPos++;
                extensibles[myPos] = vertexMap[lastSourceId];
                myPos++;
                break;
            }
        }
        for (int i = lastIndex; i > 0; i--) {
            if (edges[i - 1].getType() == true && edges[i - 1].getDestId() == lastSourceId) {
                lastSourceId = edges[i - 1].getSrcId();
                extensibles[myPos] = vertexMap[lastSourceId];
                myPos++;
            }
        }
        extensibles[myPos] = -1;//Indication that we finished.

        return extensibles;

    }

    @Override
    public void generateMinPatternCode() {

        if (edgesMinCode == null) {
            if (adjListMap == null) {
                computeAdjacencyList();
            }

            autoVertexSet = new HashIntSet[numberOfVertices];
            for (int i = 0; i < numberOfVertices; i++)
                autoVertexSet[i] = HashIntSets.newMutableSet();

            numberOfEdgesMinCode = numberOfEdges;
            numberOfVerticesMinCode = numberOfVertices;

            edgesMinCode = edges;
            vertexMapMinCode = vertexMap;

            //copy the original vertex map to reconstruct the autovertexset
            int originalVertexMap[] = Arrays.copyOf(vertexMap, numberOfVertices);

            edges = new PatternEdge[numberOfEdges];
            vertexMap = new int[numberOfEdges + 1];

            numberOfEdges = 0;
            numberOfVertices = 0;

            internalGenerateMinPatternCode();

            edges = edgesMinCode;
            vertexMap = vertexMapMinCode;
            numberOfEdges = numberOfEdgesMinCode;
            numberOfVertices = numberOfVerticesMinCode;
            convertAutoVertexSet(originalVertexMap, numberOfVertices);
        }
    }

    public void internalGenerateMinPatternCode() {
        int[] extensibleVertexIds = this.getExtensibleVertexIdsDFS();

        if (extensibleVertexIds == null) {
            IntIntCursor cursor = adjListMap.cursor();
            while (cursor.moveNext()) {
                int srcId = cursor.key();
                int srcLabel = adjListLabels[cursor.value()];
                int[] neighbors = adjListDst[cursor.value()];

                int kkpos = 0;
                while (neighbors[kkpos] >= 0) {
                    int destId = neighbors[kkpos];
                    kkpos++;
                    int destLabel = getLabelFromAdjList(destId);
                    if (this.addEdge(srcId, srcLabel, destId, destLabel)) {
                        int comp = compareEdgeCode(edges, numberOfEdges, edgesMinCode, numberOfEdgesMinCode);
                        if (comp < 0) {
                            if (numberOfEdges == numberOfEdgesMinCode) {
                                copyToMin();
                                //update autoVertexSet
                                for (int j = 0; j < numberOfVerticesMinCode; j++) {
                                    autoVertexSet[j].clear();
                                    autoVertexSet[j].add(vertexMapMinCode[j]);
                                }
                            }
                            internalGenerateMinPatternCode();
                        } else if (comp == 0) {
                            //update autoVertexSet
                            for (int j = 0; j < numberOfVerticesMinCode; j++) {
                                autoVertexSet[j].add(vertexMap[j]);
                            }
                        }
                        this.removeLastEdge();
                    }
                }
            }
        } else {
            int k = 0;
            while (extensibleVertexIds[k] >= 0) {
                int i = extensibleVertexIds[k];
                k++;
                int srcId = i;
                int srcLabel = getLabelFromAdjList(i);
                int[] neighbors = adjListDst[adjListMap.get(srcId)];
                int pppp = 0;
                while (neighbors[pppp] >= 0) {
                    int destId = neighbors[pppp];
                    pppp++;
                    int destLabel = getLabelFromAdjList(destId);

                    if (this.addEdge(srcId, srcLabel, destId, destLabel)) {
                        int comp = compareEdgeCode(edges, numberOfEdges, edgesMinCode, numberOfEdgesMinCode);
                        if (comp < 0) {
                            if (numberOfEdges == numberOfEdgesMinCode) {
                                copyToMin();
                                //update autoVertexSet
                                for (int j = 0; j < numberOfVerticesMinCode; j++) {
                                    autoVertexSet[j].clear();
                                    autoVertexSet[j].add(vertexMapMinCode[j]);
                                }
                            }
                            internalGenerateMinPatternCode();
                        } else if (comp == 0) {
                            //update autoVertexSet
                            for (int j = 0; j < numberOfVerticesMinCode; j++) {
                                autoVertexSet[j].add(vertexMap[j]);
                            }
                        }
                        this.removeLastEdge();
                    }
                }
            }
        }
    }

    private int getLabelFromAdjList(int destId) {
        return adjListLabels[adjListMap.get(destId)];
    }

    private int stupidAdd(int src, int src_label, int to_add1, int to_add2, int localpos, int[] localPos, int max_size) {
        if (!adjListMap.containsKey(src)) {
            adjListMap.put(src, localpos);
            adjListLabels[localpos] = src_label;
            adjListDst[localpos] = new int[max_size + 1];
            adjListDst[localpos][0] = to_add1;
            localPos[localpos] = 1; //Current position to insert.
            localpos++;
        } else {
            int p1 = adjListMap.get(src);
            adjListDst[p1][localPos[p1]] = to_add2;
            localPos[p1] = localPos[p1] + 1;
        }

        return localpos;
    }

    /**
     * Blah
     *
     * @param embedding
     * @return
     */
    public void computeEmbeddingAdjancyList(Embedding embedding) {

        int numberOfVertices = embedding.getNumVertices();

        adjListMap = HashIntIntMaps.newMutableMap(numberOfVertices);
        adjListDst = new int[numberOfVertices][];
        adjListLabels = new int[numberOfVertices];
        int localpos = 0;

        int localPos[] = new int[numberOfVertices];

        int numberOfEdges = embedding.getNumEdges();
        int[] edges = embedding.getEdges();

        for (int i = 0; i < numberOfEdges; i++) {
            Edge<?> e = g.getEdge(edges[i]);
            Vertex<?> src = g.getVertex(e.getSourceId());
            Vertex<?> dest = g.getVertex(e.getDestinationId());

            localpos = stupidAdd(src.getVertexId(), src.getVertexLabel(), dest.getVertexId(), e.getDestinationId(),
                    localpos, localPos, numberOfVertices + 1);

            localpos = stupidAdd(dest.getVertexId(), dest.getVertexLabel(), src.getVertexId(), src.getVertexId(),
                    localpos, localPos, numberOfVertices + 1);

        }

        // Make sure we finalize the dest properly with -1.
        for (int i = 0; i < numberOfVertices; i++) {
            if (adjListDst[i] == null) {
                //No more entries...
                break;
            }
            adjListDst[i][localPos[i]] = -1;
        }
    }

    public void computeAdjacencyList() {

        adjListMap = HashIntIntMaps.newMutableMap(numberOfVertices);
        adjListDst = new int[numberOfVertices][];
        adjListLabels = new int[numberOfVertices];
        int localpos = 0;

        int localPos[] = new int[numberOfVertices + 1];


        for (int i = 0; i < numberOfEdges; i++) {
            PatternEdge e = edges[i];

            localpos = stupidAdd(vertexMap[e.getSrcId()], e.getSrcLabel(),
                    vertexMap[e.getDestId()], vertexMap[e.getDestId()], localpos, localPos, numberOfVertices + 1);

            localpos = stupidAdd(vertexMap[e.getDestId()], e.getDestLabel(),
                    vertexMap[e.getSrcId()], vertexMap[e.getSrcId()], localpos, localPos, numberOfVertices + 1);

        }
        // Make sure we finalize the dest properly with -1.
        for (int i = 0; i < numberOfVertices; i++) {
            if (adjListDst[i] == null) {
                //No more entries...
                break;
            }
            adjListDst[i][localPos[i]] = -1;
        }
    }

    public static boolean isEdgeCodeSmaller(PatternEdge edges1[], int numEdges1, PatternEdge edges2[], int numEdges2) {
        boolean isSmaller = false;
        boolean isSimilar = true;

        int minSize = Math.min(numEdges1, numEdges2);

        for (int i = 0; i < minSize; i++) {
            if (!edges1[i].equals(edges2[i])) {
                if (edges1[i].isSmaller(edges2[i])) {
                    isSmaller = true;
                } else {
                    isSmaller = false;
                }
                isSimilar = false;
                break;
            }
        }

        if (isSimilar) {
            if (numEdges1 < numEdges2) {
                isSmaller = false;
            } else {
                isSmaller = true;
            }
        }

        return isSmaller;
    }

    public static int compareEdgeCode(PatternEdge edges1[], int numEdges1, PatternEdge edges2[], int numEdges2) {
        int minSize = Math.min(numEdges1, numEdges2);
        int ret = 0;
        for (int i = 0; i < minSize; i++) {
            ret = edges1[i].compareTo(edges2[i]);
            if (ret != 0) {
                break;
            }
        }
        if (ret < 0) {
            if (numEdges1 < numEdges2)
                return -2;
            else
                return -3;
        } else if (ret > 0) {
            if (numEdges1 < numEdges2)
                return 2;
            else
                return 3;
        } else {
            if (numEdges1 == numEdges2)
                return 0;
            else if (numEdges1 < numEdges2)
                return -1;
            else
                return 1;
        }
    }

    public static boolean isEdgeCodeSub(PatternEdge edges1[], int numEdges1, PatternEdge edges2[], int numEdges2) {
        boolean isSubcode = true;

        if (numEdges1 > numEdges2) {
            isSubcode = false;
        } else {
            int i = 0;
            while (i < numEdges1 && i < numEdges2) {
                if (!edges1[i].equals(edges2[i])) {
                    isSubcode = false;
                    break;
                }
                i++;
            }
        }
        return isSubcode;
    }

    public boolean isPatternWithEdges(String edges) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfEdges; i++) {
            sb.append(this.edges[i] + " ");
        }
        return edges.equals(sb.toString());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("numberOfEdges: " + numberOfEdges + "\n");
        for (int i = 0; i < numberOfEdges; i++) {
            sb.append(edges[i] + " ");
        }
        sb.append("\n");
        sb.append("numberOfVertices: " + numberOfVertices + "\n");
        for (int i = 0; i < numberOfVertices; i++) {
            sb.append("[" + i + "," + vertexMap[i] + "] ");
        }
        if (autoVertexSet != null) {
            sb.append("autoVertexSet:");
            for (int i = 0; i < numberOfVertices; i++) {
                IntCursor cursor = autoVertexSet[i].cursor();
                sb.append("\nidx: " + i);
                while (cursor.moveNext()) {
                    sb.append("\n" + cursor.elem());
                }

            }
        }
        sb.append("\n");
        sb.append("hashcode: " + this.hashCode());
        sb.append("\n");
        return sb.toString();
    }

    public String toStringVertexMap() {
        StringBuilder sb = new StringBuilder();
        sb.append("Pattern VertexMap:");

        for (int i = 0; i < numberOfVertices; i++) {
            sb.append(" " + vertexMap[i]);
        }

        sb.append("\n");
        return sb.toString();
    }

    public int[] getPatternCode() {
        int[] ccode = new int[numberOfEdgesMinCode * 5];

        for (int i = 0; i < numberOfEdgesMinCode; i++) {
            ccode[i * 5] = edgesMinCode[i].getSrcId();
            ccode[i * 5 + 1] = edgesMinCode[i].getSrcLabel();
            ccode[i * 5 + 2] = edgesMinCode[i].getDestId();
            ccode[i * 5 + 3] = edgesMinCode[i].getDestLabel();
            ccode[i * 5 + 4] = edgesMinCode[i].getTypeInt();
        }

        return ccode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicPattern Pattern = (BasicPattern) o;

        if (numberOfEdges != Pattern.numberOfEdges) return false;
        if (numberOfVertices != Pattern.numberOfVertices) return false;

        for (int i = 0; i < numberOfEdges; ++i) {
            if (!edges[i].equals(Pattern.edges[i])) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = numberOfVertices;
        result = 31 * result + numberOfEdges;
        for (int i = 0; i < numberOfEdges; ++i) {
            PatternEdge edge = edges[i];
            result = 31 * result + (edge != null ? edge.hashCode() : 0);
        }
        return result;
    }

    public void copyToMin() {
        for (int j = 0; j < numberOfEdges; j++) {
            edgesMinCode[j].setSrcId(edges[j].getSrcId());
            edgesMinCode[j].setSrcLabel(edges[j].getSrcLabel());
            edgesMinCode[j].setDestId(edges[j].getDestId());
            edgesMinCode[j].setDestLabel(edges[j].getDestLabel());
            edgesMinCode[j].setType(edges[j].getType());
        }
        for (int j = 0; j < numberOfVertices; j++) {
            vertexMapMinCode[j] = vertexMap[j];
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(numberOfEdges);
        for (int i = 0; i < numberOfEdges; i++) {
            edges[i].write(out);
        }
        out.writeInt(numberOfVertices);
        for (int i = 0; i < numberOfVertices; i++) {
            out.writeInt(vertexMap[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        numberOfEdges = in.readInt();
        edges = new PatternEdge[numberOfEdges];
        for (int i = 0; i < numberOfEdges; i++) {
            edges[i] = new PatternEdge();
            edges[i].readFields(in);
        }

        numberOfVertices = in.readInt();
        vertexMap = new int[numberOfEdges + 1];
        for (int i = 0; i < numberOfVertices; i++) {
            vertexMap[i] = in.readInt();
        }
    }

    public PatternEdge[] getEdges() {
        return edges;
    }

    @Override
    public HashIntIntMap getCanonicalLabeling() {
        throw new NotImplementedException();
    }
}