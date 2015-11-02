package io.arabesque.pattern;

import io.arabesque.graph.Vertex;
import io.arabesque.utils.IntArrayList;
import io.arabesque.utils.ObjArrayList;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.util.Collection;

public class VICPattern extends BasicPattern {
    private static final Logger LOG = Logger.getLogger(VICPattern.class);

    // Underlying pattern (the one represented by BasicPattern) extra stuff {{
    // Index = underlying vertex position, Value = vertex label
    private IntArrayList underlyingPosToLabel;
    // Index = underlying vertex position, Value = list of neighbour ids of that vertex
    private ObjArrayList<IntArrayList> underlyingAdjacencyList;
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
    // Index = tmp vertex position, Value = list of pattern edges added when we added the associated tmp vertex position
    private ObjArrayList<PatternEdgeArrayList> tmpEdges;

    private ObjArrayList<IntSet> intSetPool;
    private ObjArrayList<PatternEdgeArrayList> patternEdgeArrayListPool;
    // }}

    // Minimum pattern stuff (the smallest one we found while searching for the canonical version) {{
    // True = We found a minimum and filled the minimum related structures with actual data. False otherwise.
    private boolean foundMinimum;
    // Labelling of our temporary pattern
    // Key = position in underlying pattern
    // Value = position in minimum pattern
    private IntIntMap minLabelling;
    // Inverse labelling of our temporary pattern
    // Key = position in minimum pattern
    // Value = position in underlying pattern
    private IntIntMap minInverseLabelling;
    // Edges of our minimum pattern
    // Index = min vertex position, Value = list of pattern edges added when we added the associated min vertex position
    private ObjArrayList<PatternEdgeArrayList> minEdges;
    // Vertex position equivalences
    private VertexPositionEquivalences vertexPositionEquivalences;
    // }}

    private boolean dirtyAdjacencyList;
    private boolean dirtyMinimumStuff;

    public VICPattern() {
        super();
    }

    public VICPattern(VICPattern other) {
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

        if (underlyingAdjacencyList == null) {
            underlyingAdjacencyList = new ObjArrayList<>(numVertices);
        }
        else {
            reclaimIntArrayLists(underlyingAdjacencyList);
            underlyingAdjacencyList.clear();
            underlyingAdjacencyList.ensureCapacity(numVertices);
        }

        for (int i = 0; i < numVertices; ++i) {
            underlyingAdjacencyList.add(createIntArrayList());
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

            underlyingAdjacencyList.get(srcPos).add(dstPos);
            underlyingAdjacencyList.get(dstPos).add(srcPos);
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
            tmpEdges = new ObjArrayList<>(numVertices);
        }
        else {
            reclaimPatternEdgeArrayLists(tmpEdges);
            tmpEdges.clear();
            tmpEdges.ensureCapacity(numEdges);
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

        if (minInverseLabelling == null) {
            minInverseLabelling = positionMapFactory.newMutableMap(numVertices);
        }
        else {
            minInverseLabelling.clear();
            minInverseLabelling.ensureCapacity(numVertices);
        }

        if (minEdges == null) {
            minEdges = new ObjArrayList<>(numVertices);
        }
        else {
            reclaimPatternEdgeArrayLists(minEdges);
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

        for (int i = 0; i < numVertices; ++i) {
            minEdges.add(createPatternEdgeArrayList());
        }

        foundMinimum = false;
    }

    private void resetAuxStructures() {
        buildAdjacencyList();
        resetTmpStructures();
        resetMinStructures();
    }

    @Override
    public Pattern copy() {
        return new VICPattern(this);
    }

    @Override
    public boolean turnCanonical() {
        boolean changed = super.turnCanonical();

        /*if (changed) {
            vertexPositionEquivalences.convertBasedOnRelabelling(minLabelling);
        }*/

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
        //vertexPositionEquivalences.propagateEquivalences();
        dirtyMinimumStuff = false;
    }

    private void debugTmp() {
        StringBuilder strBuilder = new StringBuilder();

        strBuilder.append("TMP: {" );

        strBuilder.append(" edges=[");
        strBuilder.append(StringUtils.join(", ", tmpEdges));
        strBuilder.append("], labelling=");
        strBuilder.append(tmpLabelling);
        strBuilder.append("}");

        System.out.println(strBuilder.toString());
    }

    private void debugMin() {
        StringBuilder strBuilder = new StringBuilder();

        strBuilder.append("MIN: {" );

        strBuilder.append(" edges=[");
        strBuilder.append(StringUtils.join(", ", minEdges));
        strBuilder.append("], labelling=");
        strBuilder.append(minLabelling);
        strBuilder.append("}");

        System.out.println(strBuilder.toString());
    }

    /**
     * Find the canonical labelling by increasing a tmpPattern one vertex at a time and comparing it with the
     * previous minimum.
     *
     * @param tmpPreviouslyEqualToMin True if up until now this tmpPattern is equal to the current minimum
     */
    private void _findCanonicalLabelling(boolean tmpPreviouslyEqualToMin) {
        IntArrayList underlyingVertexPosThatExtendTmp = getUnderlyingVertexPosThatExtendTmp();
        IntCursor underlyingVertexPosThatExtendTmpCursor = underlyingVertexPosThatExtendTmp.cursor();

        // TODO: Debug
        //debugTmp();

        // For each underlying vertex position we can add as the next tmp vertex position...
        while (underlyingVertexPosThatExtendTmpCursor.moveNext()) {
            int underlyingVertexPosToAdd = underlyingVertexPosThatExtendTmpCursor.elem();

            // Add it...
            int newTmpVertexPos = addTmpVertex(underlyingVertexPosToAdd);
            int newTmpVertexLabel = underlyingPosToLabel.get(underlyingVertexPosToAdd);

            // TODO: Debug
            //System.out.println("Trying to expand by adding underlying pos " + underlyingVertexPosToAdd + " at new tmp pos " + newTmpVertexPos + " (" + newTmpVertexLabel + ")");

            // And find its neighbours
            IntArrayList neighbourUnderlyingPositions = underlyingAdjacencyList.get(underlyingVertexPosToAdd);
            IntCursor neighbourUnderlyingPositionsCursor = neighbourUnderlyingPositions.cursor();

            PatternEdgeArrayList edgesToAdd = createPatternEdgeArrayList();

            // For each neighbour of the new tmp vertex position
            while (neighbourUnderlyingPositionsCursor.moveNext()) {
                int neighbourUnderlyingPos = neighbourUnderlyingPositionsCursor.elem();

                int neighbourTmpPos = tmpLabelling.get(neighbourUnderlyingPos);

                // Ignore all edges that connect to underlying vertices we still haven't added.
                if (neighbourTmpPos == -1) {
                    continue;
                }

                int neighbourLabel = underlyingPosToLabel.get(neighbourUnderlyingPos);

                // Add edge connecting existing tmp vertex to new tmp vertex
                PatternEdge newEdge = createPatternEdge(neighbourTmpPos, neighbourLabel, newTmpVertexPos, newTmpVertexLabel, true);

                edgesToAdd.add(newEdge);
            }

            // TODO: Debug
            //System.out.println("Which means adding the following edges: " + edgesToAdd);

            // If adding this new vertex position is valid (it is connected to previous vertex positions or is the first one)
            if (edgesToAdd.size() > 0 || newTmpVertexPos == 0) {
                edgesToAdd.sort();

                // Is this tmp pattern we are constructing promising?
                // Promising = potentially smaller than current minPattern
                // It automatically is if we got this far (otherwise we would have stopped at the previous step)
                boolean promisingTmpPattern = true;

                // Is this tmp pattern we are constructing equal to min (up until now)?
                // It is if it previously was
                boolean equalToMinTmpPattern = tmpPreviouslyEqualToMin;

                // If we already found a minimum, compare this addition with the minimum
                if (foundMinimum) {
                    int comparisonResult;

                    // If this is not the first vertex (we actually added edges, compare the edges)
                    if (newTmpVertexPos > 0) {
                        PatternEdgeArrayList minEquivalentEdges = minEdges.get(newTmpVertexPos);

                        comparisonResult = edgesToAdd.compareTo(minEquivalentEdges);
                    }
                    // Else, if this is the first vertex, check if the labels match
                    else {
                        int minFirstUnderlyingVertexPos = minInverseLabelling.get(newTmpVertexPos);
                        int minFirstUnderylingVertexLabel = underlyingPosToLabel.get(minFirstUnderlyingVertexPos);

                        comparisonResult = Integer.compare(newTmpVertexLabel, minFirstUnderylingVertexLabel);
                    }

                    // Is this tmp pattern we are constructing equal to min (up until now)?
                    // It is if it previously was and these edges we are about to add also are equal
                    equalToMinTmpPattern = equalToMinTmpPattern && comparisonResult == 0;

                    // If we were equal to the min pattern up until now but no longer are, then this last
                    // edge will tell us if this is a promising path or not.
                    if (tmpPreviouslyEqualToMin && !equalToMinTmpPattern) {
                        // If these new edges are leading to a higher pattern, tmp pattern no longer looks promising
                        if (comparisonResult > 0) {
                            promisingTmpPattern = false;
                        }
                    }
                }

                // TODO: Debug
                //System.out.println("promising=" + promisingTmpPattern + ", foundMinimum=" + foundMinimum + ", equalToMinTmpPattern=" + equalToMinTmpPattern);

                // If the tmp pattern is still promising...
                if (promisingTmpPattern) {

                    // Commit edges to tmp
                    addTmpEdges(edgesToAdd);

                    // And if we reached the target size...
                    if (tmpLabelling.size() == getNumberOfVertices()) {
                        // Then this tmp becomes the new min (unless it already is)
                        if (!equalToMinTmpPattern || !foundMinimum) {
                            // TODO:Debug
                            //System.out.println("copied to minimum!");
                            copyTmpToMin();
                            // TODO:Debug
                            //debugMin();
                            // Since we found a new minimum, clear previous vertex equivalences
                            vertexPositionEquivalences.clear();
                        }

                        /* We'll reach the minimum x times where x is the number of automorphisms that exist.
                         * Each automorphism corresponds to a different vertex equivalence.
                         *
                         * Example:
                         *     (label:1)<->(label:1)<->(label:1)
                         *
                         *     We'll reach 2 tmp patterns that match the 1-1-1 min:
                         *     - One by adding the right edge first and the left edge after.
                         *     - One by adding the left edge first and the right edge after.
                         *
                         * Only 1 of these x times will enter the above if and clear the vertexPositionEquivalences.
                         * The others will simply add equivalences as done below.
                         *
                         */
                        // Add this new equivalence based on the tmp labelling
                        // TODO: Debug
                        //System.out.println("!!!!Equal to minimum!!!!");
                        // TODO: Debug
                        //System.out.println("vertexPositionEquivalances=" + vertexPositionEquivalences);
                        IntIntCursor tmpLabellingCursor = tmpLabelling.cursor();

                        while (tmpLabellingCursor.moveNext()) {
                            int underlyingPos = tmpLabellingCursor.key();
                            int tmpEquivalentPos = tmpLabellingCursor.value();
                            int underlyingPosAccordingToMin = minInverseLabelling.get(tmpEquivalentPos);
                            vertexPositionEquivalences.addEquivalence(underlyingPos, underlyingPosAccordingToMin);
                        }

                        // TODO: Debug
                        //System.out.println("vertexPositionEquivalances (after)=" + vertexPositionEquivalences);
                    }
                    // If it's promising and we still haven't reached the target size, continue!
                    else {
                        _findCanonicalLabelling(equalToMinTmpPattern);
                    }

                    removeLastTmpEdges();
                }
                // If not prosiming, discard these edges
                else {
                    reclaimPatternEdgeArrayList(edgesToAdd);
                }
            }

            removeLastTmpVertex();
        }

        reclaimIntArrayList(underlyingVertexPosThatExtendTmp);
    }

    private void copyTmpToMin() {
        int numVertices = tmpLabelling.size();

        for (int i = 0; i < numVertices; ++i) {
            PatternEdgeArrayList tmpEdgesAddedByPos = tmpEdges.get(i);
            PatternEdgeArrayList minEdgesAddedByPos = minEdges.get(i);

            reclaimPatternEdges(minEdgesAddedByPos);
            minEdgesAddedByPos.clear();

            for (int j = 0; j < tmpEdgesAddedByPos.size(); ++j) {
                minEdgesAddedByPos.add(createPatternEdge(tmpEdgesAddedByPos.get(j)));
            }
        }

        minLabelling.clear();
        minLabelling.putAll(tmpLabelling);

        minInverseLabelling.clear();
        minInverseLabelling.putAll(tmpInverseLabelling);

        foundMinimum = true;
    }

    private void addTmpEdges(PatternEdgeArrayList edgesToAdd) {
        tmpEdges.add(edgesToAdd);
    }

    private void removeLastTmpEdges() {
        reclaimPatternEdgeArrayList(tmpEdges.remove(tmpEdges.size() - 1));
    }

    private int addTmpVertex(int underlyingPos) {
        int tmpPos = tmpLabelling.size();
        tmpLabelling.put(underlyingPos, tmpPos);
        tmpInverseLabelling.put(tmpPos, underlyingPos);
        return tmpPos;
    }

    private void removeLastTmpVertex() {
        int underlyingPos = tmpInverseLabelling.remove(tmpInverseLabelling.size() - 1);
        tmpLabelling.remove(underlyingPos);
    }

    private IntArrayList getUnderlyingVertexPosThatExtendTmp() {
        int numUnderlyingVertices = getNumberOfVertices();

        IntArrayList underlyingVertexPosThatExtendTmp = createIntArrayList();
        underlyingVertexPosThatExtendTmp.ensureCapacity(numUnderlyingVertices);

        for (int i = 0; i < numUnderlyingVertices; ++i) {
            if (!tmpLabelling.containsKey(i)) {
                underlyingVertexPosThatExtendTmp.add(i);
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

    protected PatternEdgeArrayList createPatternEdgeArrayList() {
        if (patternEdgeArrayListPool != null && !patternEdgeArrayListPool.isEmpty()) {
            return patternEdgeArrayListPool.remove(patternEdgeArrayListPool.size() - 1);
        } else {
            return new PatternEdgeArrayList();
        }
    }

    protected void reclaimPatternEdgeArrayList(PatternEdgeArrayList patternEdgeArrayList) {
        if (patternEdgeArrayListPool == null) {
            patternEdgeArrayListPool = new ObjArrayList<>();
        }

        reclaimPatternEdges(patternEdgeArrayList);
        patternEdgeArrayList.clear();

        patternEdgeArrayListPool.add(patternEdgeArrayList);
    }

    protected void reclaimPatternEdgeArrayLists(Collection<PatternEdgeArrayList> patternEdgeArrayLists) {
        if (patternEdgeArrayListPool == null) {
            patternEdgeArrayListPool = new ObjArrayList<>(patternEdgeArrayLists.size());
        }

        patternEdgeArrayListPool.ensureCapacity(patternEdgeArrayListPool.size() + patternEdgeArrayLists.size());

        for (PatternEdgeArrayList patternEdgeArrayList : patternEdgeArrayLists) {
            reclaimPatternEdgeArrayList(patternEdgeArrayList);
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
