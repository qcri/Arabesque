package io.arabesque.pattern;

import io.arabesque.graph.Vertex;
import io.arabesque.utils.IntArrayList;
import io.arabesque.utils.ObjArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.IntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import org.apache.log4j.Logger;

import java.util.Collection;

public class BACPattern extends BasicPattern {
    private static final Logger LOG = Logger.getLogger(BACPattern.class);

    // Underlying pattern (the one represented by BasicPattern) extra stuff {{
    // Index = underlying vertex position, Value = vertex label
    private IntArrayList underlyingPosToLabel;
    // Index = underlying vertex position, Value = list of neighbour ids of that vertex
    private ObjArrayList<IntArrayList> patternAdjacencyList;
    // IntArrayList pool to reduce # of allocations
    private ObjArrayList<IntArrayList> intArrayListPool;
    // }}

    // Temporary pattern stuff (the one we construct to find the canonical relabelling) {{
    // Labelling of our temporary pattern
    // Key = position in underlying pattern
    // Value = position in temporary pattern
    private IntIntMap tmpLabelling;
    // Inverse labelling of our temporary pattern
    // Key = position in temporary pattern
    // Value = position in underlying pattern
    private IntIntMap tmpInverseLabelling;
    // Edges of our temporary pattern
    private PatternEdgeArrayList tmpEdges;
    // Connection cache
    // K = position in temporary pattern
    // V = set of positions in temporary pattern to which the position K is connected to
    // If a mapping (k, {..., v, ...}) exists in the map, then there is an edge connecting k to v in the tmp pattern
    private IntObjMap<IntSet> tmpConnectionCache;
    // List of vertex positions from which we should expand the current temporary pattern
    private IntArrayList underlyingVertexPosThatExtendTmp;
    private ObjArrayList<IntSet> intSetPool;
    // }}

    // Minimum pattern stuff (the smallest one we found while searching for the canonical version) {{
    // Labelling of our temporary pattern
    // Key = position in underlying pattern
    // Value = position in minimum pattern
    private IntIntMap minLabelling;
    // Edges of our minimum pattern
    private PatternEdgeArrayList minEdges;
    // Vertex position equivalences
    private VertexPositionEquivalences vertexPositionEquivalences;
    // }}

    private boolean dirtyAdjacencyList;
    private boolean dirtyMinimumStuff;

    public BACPattern() {
        super();
    }

    public BACPattern(BACPattern other) {
        super(other);
    }

    @Override
    protected void setDirty() {
        super.setDirty();

        dirtyAdjacencyList = true;
        dirtyMinimumStuff = true;
    }

    private void resetAdjacencyList() {
        int numVertices = getNumberOfVertices();

        if (underlyingPosToLabel == null) {
            underlyingPosToLabel = new IntArrayList(numVertices);
        }
        else {
            underlyingPosToLabel.clear();
            underlyingPosToLabel.ensureCapacity(numVertices);
        }

        if (patternAdjacencyList == null) {
            patternAdjacencyList = new ObjArrayList<>(numVertices);
        }
        else {
            reclaimIntArrayLists(patternAdjacencyList);
            patternAdjacencyList.clear();
            patternAdjacencyList.ensureCapacity(numVertices);
        }

        for (int i = 0; i < numVertices; ++i) {
            patternAdjacencyList.add(createIntArrayList());
        }
    }

    private void buildAdjacencyList() {
        if (!dirtyAdjacencyList) {
            return;
        }

        resetAdjacencyList();

        IntCursor vertexIdCursor = getVertices().cursor();

        while (vertexIdCursor.moveNext()) {
            int vertexId = vertexIdCursor.elem();
            Vertex<?> vertex = mainGraph.getVertex(vertexId);
            underlyingPosToLabel.add(vertex.getVertexLabel());
        }

        for (PatternEdge edge : getEdges()) {
            int srcPos = edge.getSrcId();
            int dstPos = edge.getDestId();

            patternAdjacencyList.get(srcPos).add(dstPos);
            patternAdjacencyList.get(dstPos).add(srcPos);
        }
    }

    private void resetTmpStructures() {
        int numVertices = getNumberOfVertices();
        int numEdges = getNumberOfEdges();

        if (tmpLabelling == null) {
            tmpLabelling = positionMapFactory.newMutableMap(numVertices);
        }
        else {
            tmpLabelling.clear();
            tmpLabelling.ensureCapacity(numVertices);
        }

        if (tmpInverseLabelling == null) {
            tmpInverseLabelling = positionMapFactory.newMutableMap(numVertices);
        }
        else {
            tmpInverseLabelling.clear();
            tmpInverseLabelling.ensureCapacity(numVertices);
        }

        if (tmpEdges == null) {
            tmpEdges = new PatternEdgeArrayList(numEdges);
        }
        else {
            reclaimPatternEdges(tmpEdges);
            tmpEdges.clear();
            tmpEdges.ensureCapacity(numEdges);
        }

        if (underlyingVertexPosThatExtendTmp == null) {
            underlyingVertexPosThatExtendTmp = new IntArrayList(numVertices);
        }
        else {
            underlyingVertexPosThatExtendTmp.clear();
            underlyingVertexPosThatExtendTmp.ensureCapacity(numVertices);
        }

        if (tmpConnectionCache == null) {
            tmpConnectionCache = HashIntObjMaps.getDefaultFactory().newMutableMap(numVertices);
        }
        else {
            reclaimIntSets(tmpConnectionCache.values());
            tmpConnectionCache.clear();
            tmpConnectionCache.ensureCapacity(numVertices);
        }

        for (int i = 0; i < numVertices; ++i) {
            tmpConnectionCache.put(i, createIntSet());
        }
    }

    private void resetMinStructures() {
        dirtyMinimumStuff = true;

        int numVertices = getNumberOfVertices();
        int numEdges = getNumberOfEdges();

        if (minLabelling == null) {
            minLabelling = positionMapFactory.newMutableMap(numVertices);
        }
        else {
            minLabelling.clear();
            minLabelling.ensureCapacity(numVertices);
        }

        if (minEdges == null) {
            minEdges = new PatternEdgeArrayList(numEdges);
        }
        else {
            reclaimPatternEdges(minEdges);
            minEdges.clear();
            minEdges.ensureCapacity(numEdges);
        }

        if (vertexPositionEquivalences == null) {
            vertexPositionEquivalences = new VertexPositionEquivalences();
        }
        else {
            vertexPositionEquivalences.clear();
        }

        vertexPositionEquivalences.setNumVertices(numVertices);

        // Minimum starts equal to the current underlying pattern
        for (int i = 0; i < numVertices; ++i) {
            minLabelling.put(i, i);
        }
        minEdges.addAll(getEdges());
    }

    private void resetAuxStructures() {
        buildAdjacencyList();
        resetTmpStructures();
        resetMinStructures();
    }

    @Override
    public Pattern copy() {
        return new BACPattern(this);
    }

    @Override
    public boolean turnCanonical() {
        boolean changed = super.turnCanonical();

        if (changed) {
            vertexPositionEquivalences.convertBasedOnRelabelling(minLabelling);
        }

        return changed;
    }

    @Override
    protected void fillVertexPositionEquivalences(VertexPositionEquivalences vertexPositionEquivalences) {
        findCanonicalLabelling();
        vertexPositionEquivalences.addAll(this.vertexPositionEquivalences);
    }

    @Override
    protected void fillCanonicalLabelling(IntIntMap canonicalLabelling) {
        findCanonicalLabelling();
        canonicalLabelling.putAll(minLabelling);
    }

    private void findCanonicalLabelling() {
        if (!dirtyMinimumStuff) {
            return;
        }

        resetAuxStructures();
        _findCanonicalLabelling(true);
        vertexPositionEquivalences.propagateEquivalences();
    }

    private void _findCanonicalLabelling(boolean tmpPreviouslyEqualToMin) {
        IntCollection underlyingVertexPosThatExtendTmp = getUnderlyingVertexPosThatExtendTmp();
        IntCursor underlyingVertexPosThatExtendTmpCursor = underlyingVertexPosThatExtendTmp.cursor();

        int posEdgeBeingAdded = tmpEdges.size();

        while (underlyingVertexPosThatExtendTmpCursor.moveNext()) {
            int currentUnderlyingPos = underlyingVertexPosThatExtendTmpCursor.elem();
            int currentLabel = underlyingPosToLabel.get(currentUnderlyingPos);

            IntArrayList neighbourUnderlyingPositions = patternAdjacencyList.get(currentUnderlyingPos);
            IntCursor neighbourUnderlyingPositionsCursor = neighbourUnderlyingPositions.cursor();

            while (neighbourUnderlyingPositionsCursor.moveNext()) {
                int neighbourUnderlyingPos = neighbourUnderlyingPositionsCursor.elem();
                int neighbourLabel = underlyingPosToLabel.get(neighbourUnderlyingPos);

                PatternEdge newEdge = addTmpEdge(currentUnderlyingPos, currentLabel, neighbourUnderlyingPos, neighbourLabel);

                // If insertion failed
                if (newEdge == null) {
                    continue;
                }

                // Is this tmp pattern we are constructing promising?
                // Promising = potentially smaller than current minPattern
                // It automatically is if we got this far (otherwise we would have stopped at the previous step)
                boolean promisingTmpPattern = true;

                PatternEdge correspondingMinEdge = minEdges.get(posEdgeBeingAdded);

                int comparisonResult = newEdge.compareTo(correspondingMinEdge);

                // Is this tmp pattern we are constructing equal to min (up until now)?
                // It is if it previously was and the last edge is also equal
                boolean equalToMinTmpPattern = tmpPreviouslyEqualToMin && comparisonResult == 0;

                // If we were equal to the min pattern up until now but no longer are, then this last
                // edge will tell us if this is a promising path or not.
                if (tmpPreviouslyEqualToMin && !equalToMinTmpPattern) {
                    // If this new edge is leading to a higher pattern, it no longer looks promising
                    if (comparisonResult > 0) {
                        promisingTmpPattern = false;
                    }
                }

                // If the tmp pattern is promising...
                if (promisingTmpPattern) {
                    // And if we reached the target size...
                    if (tmpEdges.size() == minEdges.size()) {
                        // Then this tmp becomes the new min (unless it already is)
                        if (!equalToMinTmpPattern) {
                            copyTmpToMin();
                            // Since we found a new minimum, clear vertex equivalences
                            vertexPositionEquivalences.clear();

                            // And add this new equivalence based on the minimum labelling
                            IntIntCursor minLabellingCursor = minLabelling.cursor();

                            while (minLabellingCursor.moveNext()) {
                                int underlyingPos = minLabellingCursor.key();
                                int minEquivalentPos = minLabellingCursor.value();
                                vertexPositionEquivalences.addEquivalence(minEquivalentPos, underlyingPos);
                            }
                        }
                        /* If it is equal to the minimum, add current positions to the equivalence list.
                         *
                         * We'll reach the minimum x times where x is the number of automorphisms that exist.
                         * Each automorphism corresponds to a different vertex equivalence.
                         *
                         * Example:
                         *     (label:1)<->(label:1)<->(label:1)
                         *
                         *     We'll reach 2 tmp patterns that match the 1-1-1 min:
                         *     - One by adding the right edge first and the left edge after.
                         *     - One by adding the left edge first and the right edge after.
                         */
                        else {
                            // Add this new equivalence based on the tmp labelling
                            IntIntCursor tmpLabellingCursor = tmpLabelling.cursor();

                            while (tmpLabellingCursor.moveNext()) {
                                int underlyingPos = tmpLabellingCursor.key();
                                int tmpEquivalentPos = tmpLabellingCursor.value();
                                vertexPositionEquivalences.addEquivalence(tmpEquivalentPos, underlyingPos);
                            }
                        }
                    }
                    // If it's promising and we still haven't reached the target size, continue!
                    else {
                        _findCanonicalLabelling(equalToMinTmpPattern);
                    }
                }

                // Remove last edge addition to try another edge at this level
                removeLastTmpEdge();
            }
        }
    }

    private void copyTmpToMin() {
        int numEdges = minEdges.size();

        for (int i = 0; i < numEdges; ++i) {
            minEdges.get(i).setFromOther(tmpEdges.get(i));
        }

        minLabelling.clear();
        minLabelling.putAll(tmpLabelling);
    }

    private void removeLastTmpEdge() {
        PatternEdge tmpEdge = tmpEdges.remove(tmpEdges.size() - 1);

        // If this was a forward edge, we need to remove the last vertex
        if (tmpEdge.isForward()) {
            removeTmpVertex(tmpLabelling.size() - 1);
        }
    }

    private PatternEdge addTmpEdge(int srcPos, int srcLbl, int dstPos, int dstLbl) {
        int tmpSrcPos = tmpLabelling.get(srcPos);
        int tmpDstPos = tmpLabelling.get(dstPos);

        // If we had not seen the srcPos or the dstPos before, then this vertex
        // is disconnected from the rest of the edges in the tmp pattern
        if (tmpSrcPos == -1 && tmpDstPos == -1) {
            return null;
        }

        boolean isForward = true;

        // If we had already seen both source and destination positions, this
        // edge is either a duplicate or a 'backwards' connection.
        if (tmpSrcPos != -1 && tmpDstPos != -1) {
            // If is duplicate
            if (existsTmpEdge(tmpSrcPos, tmpDstPos)) {
                return null;
            }

            // If is backwards connection, remember this
            isForward = false;
        }
        else if (tmpSrcPos == -1) {
            tmpSrcPos = addTmpVertex(srcPos);
        }
        else {
            tmpDstPos = addTmpVertex(dstPos);
        }

        PatternEdge patternEdge = createPatternEdge(tmpSrcPos, srcLbl, tmpDstPos, dstLbl, isForward);

        tmpEdges.add(patternEdge);

        return patternEdge;
    }

    private int addTmpVertex(int underlyingPos) {
        int tmpPos = tmpLabelling.size();
        tmpLabelling.put(underlyingPos, tmpPos);
        tmpInverseLabelling.put(tmpPos, underlyingPos);
        return tmpPos;
    }

    private void removeTmpVertex(int tmpPos) {
        int underlyingPos = tmpInverseLabelling.remove(tmpPos);
        tmpLabelling.remove(underlyingPos);
    }

    private boolean existsTmpEdge(int tmpSrcPos, int tmpDstPos) {
        return tmpConnectionCache.get(tmpSrcPos).contains(tmpDstPos);
    }

    private IntCollection getUnderlyingVertexPosThatExtendTmp() {
        int numTmpVertices = tmpLabelling.size();
        int numTmpEdges = tmpEdges.size();

        if (numTmpEdges == 0) {
            return getVertexPositions();
        }

        underlyingVertexPosThatExtendTmp.clear();

        boolean first = true;
        int lastUnderlyingSourcePos = -1;

        for (int i = numTmpEdges - 1; i >= 0; i--) {
            PatternEdge edge = tmpEdges.get(i);
            int tmpSrcPos = edge.getSrcId();
            int tmpDstPos = edge.getDestId();

            int underlyingSrcPos = tmpInverseLabelling.get(tmpSrcPos);
            int underlyingDstPos = tmpInverseLabelling.get(tmpDstPos);

            if (edge.isForward()) {
                boolean edgeConnectsToPrevious = underlyingDstPos == lastUnderlyingSourcePos;
                if (first) {
                    underlyingVertexPosThatExtendTmp.add(underlyingDstPos);
                }

                if (first || edgeConnectsToPrevious) {
                    underlyingVertexPosThatExtendTmp.add(underlyingSrcPos);
                    lastUnderlyingSourcePos = underlyingSrcPos;
                }

                first = false;
            }
        }

        return underlyingVertexPosThatExtendTmp;
    }

    protected IntArrayList createIntArrayList() {
        if (intArrayListPool != null && !intArrayListPool.isEmpty()) {
            return intArrayListPool.remove(intArrayListPool.size() - 1);
        } else {
            return new IntArrayList();
        }
    }

    protected void reclaimIntArrayList(IntArrayList intArrayList) {
        if (intArrayListPool == null) {
            intArrayListPool = new ObjArrayList<>();
        }

        intArrayList.clear();

        intArrayListPool.add(intArrayList);
    }

    protected void reclaimIntArrayLists(Collection<IntArrayList> intArrayLists) {
        if (intArrayListPool == null) {
            intArrayListPool = new ObjArrayList<>(intArrayLists.size());
        }

        intArrayListPool.ensureCapacity(intArrayListPool.size() + intArrayLists.size());

        for (IntArrayList intArrayList : intArrayLists) {
            reclaimIntArrayList(intArrayList);
        }
    }

    protected IntSet createIntSet() {
        if (intSetPool != null && !intSetPool.isEmpty()) {
            return intSetPool.remove(intSetPool.size() - 1);
        } else {
            return HashIntSets.newMutableSet();
        }
    }

    protected void reclaimIntSet(IntSet intSet) {
        if (intSetPool == null) {
            intSetPool = new ObjArrayList<>();
        }

        intSet.clear();

        intSetPool.add(intSet);
    }

    protected void reclaimIntSets(Collection<IntSet> intSets) {
        if (intSetPool == null) {
            intSetPool = new ObjArrayList<>(intSets.size());
        }

        intSetPool.ensureCapacity(intSetPool.size() + intSets.size());

        for (IntSet intSet : intSets) {
            reclaimIntSet(intSet);
        }
    }
}
