package io.arabesque.pattern;

import io.arabesque.conf.Configuration;
import io.arabesque.graph.Edge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.pattern.pool.PatternEdgeArrayListPool;
import io.arabesque.pattern.pool.PatternEdgePool;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntIntMapAddConsumer;
import io.arabesque.utils.collection.ObjArrayList;
import io.arabesque.utils.pool.IntArrayListPool;
import io.arabesque.utils.pool.IntSetPool;
import io.arabesque.utils.pool.Pool;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.function.IntConsumer;
import org.apache.log4j.Logger;

public class VICPattern extends BasicPattern {
    private static final Logger LOG = Logger.getLogger(VICPattern.class);

    // Underlying pattern (the one represented by BasicPattern) extra stuff {{
    // Index = underlying vertex position, Value = vertex label
    private IntArrayList underlyingPosToLabel;
    // Index = underlying vertex position, Value = list of neighbour ids of that vertex
    private ObjArrayList<IntSet> underlyingAdjacencyList;
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

    private boolean dirtyMinimumStuff;

    private AddCandidatePatternEdgeConsumer addCandidatePatternEdgeConsumer;
    private IntIntMapAddConsumer intIntAddConsumer = new IntIntMapAddConsumer();

    public VICPattern() {
        super();
    }

    public VICPattern(VICPattern other) {
        super(other);
    }

    @Override
    protected void init() {
        super.init();
        addCandidatePatternEdgeConsumer = new AddCandidatePatternEdgeConsumer();
    }

    @Override
    protected void setDirty() {
        super.setDirty();

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

        IntSetPool intSetPool = IntSetPool.instance();

        if (underlyingAdjacencyList == null) {
            underlyingAdjacencyList = new ObjArrayList<>(numVertices);
        }
        else {
            intSetPool.reclaimObjects(underlyingAdjacencyList);
            underlyingAdjacencyList.clear();
            underlyingAdjacencyList.ensureCapacity(numVertices);
        }

        for (int i = 0; i < numVertices; ++i) {
            underlyingAdjacencyList.add(intSetPool.createObject());
        }
    }

    private void buildAdjacencyList() {
        resetAdjacencyList();

        IntCursor vertexIdCursor = getVertices().cursor();

        MainGraph mainGraph = getMainGraph();

        while (vertexIdCursor.moveNext()) {
            int vertexId = vertexIdCursor.elem();
            Vertex vertex = mainGraph.getVertex(vertexId);
            underlyingPosToLabel.add(vertex.getVertexLabel());
        }

        for (PatternEdge edge : getEdges()) {
            int srcPos = edge.getSrcPos();
            int dstPos = edge.getDestPos();

            // TODO: Handle this differently with directed edges
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
            PatternEdgeArrayListPool.instance().reclaimObjects(tmpEdges);
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
            PatternEdgeArrayListPool.instance().reclaimObjects(minEdges);
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
    protected void fillVertexPositionEquivalences(VertexPositionEquivalences vertexPositionEquivalences) {
        findCanonicalLabelling();
        vertexPositionEquivalences.addAll(this.vertexPositionEquivalences);
    }

    @Override
    protected void fillCanonicalLabelling(IntIntMap canonicalLabelling) {
        findCanonicalLabelling();
        intIntAddConsumer.setMap(canonicalLabelling);
        minLabelling.forEach(intIntAddConsumer);
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

    /**
     * Find the canonical labelling by increasing a tmpPattern one vertex at a time and comparing it with the
     * previous minimum.
     *
     * @param tmpPreviouslyEqualToMin True if up until now this tmpPattern is equal to the current minimum
     */
    protected boolean _findCanonicalLabelling(boolean tmpPreviouslyEqualToMin) {
        boolean foundNewMinimum = false;
        IntArrayList underlyingVertexPosThatExtendTmp = getUnderlyingVertexPosThatExtendTmp();
        IntCursor underlyingVertexPosThatExtendTmpCursor = underlyingVertexPosThatExtendTmp.cursor();

        // For each underlying vertex position we can add as the next tmp vertex position...
        while (underlyingVertexPosThatExtendTmpCursor.moveNext()) {
            int underlyingVertexPosToAdd = underlyingVertexPosThatExtendTmpCursor.elem();

            // Add it...
            int newTmpVertexPos = addTmpVertex(underlyingVertexPosToAdd);
            int newTmpVertexLabel = underlyingPosToLabel.getUnchecked(underlyingVertexPosToAdd);

            // And find its neighbours
            IntSet neighbourUnderlyingPositions = underlyingAdjacencyList.get(underlyingVertexPosToAdd);
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

                // Add edge(s) connecting existing tmp vertex to new tmp vertex
                addCandidatePatternEdges(edgesToAdd, neighbourUnderlyingPos, neighbourTmpPos, underlyingVertexPosToAdd, newTmpVertexPos);
            }

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
                        int minFirstUnderylingVertexLabel = underlyingPosToLabel.getUnchecked(minFirstUnderlyingVertexPos);

                        comparisonResult = Integer.compare(newTmpVertexLabel, minFirstUnderylingVertexLabel);
                    }

                    // Is this tmp pattern we are constructing equal to min (up until now)?
                    // It is if it previously was and these edges/vertex we are about to add also are equal
                    equalToMinTmpPattern = equalToMinTmpPattern && comparisonResult == 0;

                    // If we were equal to the min pattern up until now but no longer are, then this last
                    // edge/vertex will tell us if this is a promising path or not.
                    if (tmpPreviouslyEqualToMin && !equalToMinTmpPattern) {
                        // If these new edges/vertex are leading to a higher pattern, tmp pattern no longer looks promising
                        if (comparisonResult > 0) {
                            promisingTmpPattern = false;
                        }
                    }
                }

                // If the tmp pattern is still promising...
                if (promisingTmpPattern) {

                    // Commit edges to tmp
                    addTmpEdges(edgesToAdd);

                    // And if we reached the target size...
                    if (tmpLabelling.size() == getNumberOfVertices()) {
                        // Then this tmp becomes the new min (unless it already is)
                        if (!equalToMinTmpPattern || !foundMinimum) {
                            copyTmpToMin();
                            foundNewMinimum = true;
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
                        IntIntCursor tmpLabellingCursor = tmpLabelling.cursor();

                        while (tmpLabellingCursor.moveNext()) {
                            int underlyingPos = tmpLabellingCursor.key();
                            int tmpEquivalentPos = tmpLabellingCursor.value();
                            int underlyingPosAccordingToMin = minInverseLabelling.get(tmpEquivalentPos);
                            vertexPositionEquivalences.addEquivalence(underlyingPos, underlyingPosAccordingToMin);
                        }
                    }
                    // If it's promising and we still haven't reached the target size, continue!
                    else {
                        boolean foundNewMinimumInChild = _findCanonicalLabelling(equalToMinTmpPattern);

                        // If we found a new minimum in a child, then whatever was there
                        // in the previous level is now equal to the minimum (even if it wasn't before)
                        // We also have to tell our parent that we found a minimum so that he can do the same
                        if (foundNewMinimumInChild) {
                            tmpPreviouslyEqualToMin = true;
                            foundNewMinimum = true;
                        }
                    }

                    removeLastTmpEdges();
                }
                // If not prosiming, discard these edges
                else {
                    edgesToAdd.reclaim();
                }
            }

            removeLastTmpVertex();
        }

        underlyingVertexPosThatExtendTmp.reclaim();
        return foundNewMinimum;
    }

    private void addCandidatePatternEdges(PatternEdgeArrayList edgesToAdd, int neighbourUnderlyingPos, int neighbourTmpPos, int underlyingVertexPosToAdd, int newTmpVertexPos) {
        MainGraph mainGraph = getMainGraph();

        IntArrayList vertices = getVertices();

        int neighbourVertexId = vertices.getUnchecked(neighbourUnderlyingPos);
        int newVertexId = vertices.getUnchecked(underlyingVertexPosToAdd);

        addCandidatePatternEdgeConsumer.setCandidateEdgesList(edgesToAdd);
        addCandidatePatternEdgeConsumer.setNeighbourTmpPos(neighbourTmpPos);
        addCandidatePatternEdgeConsumer.setNewTmpVertexPos(newTmpVertexPos);
        addCandidatePatternEdgeConsumer.setNeighbourVertexId(neighbourVertexId);

        mainGraph.forEachEdgeId(neighbourVertexId, newVertexId, addCandidatePatternEdgeConsumer);
    }

    private void copyTmpToMin() {
        int numVertices = tmpLabelling.size();

        Pool<PatternEdge> patternEdgePool = PatternEdgePool.instance();

        for (int i = 0; i < numVertices; ++i) {
            PatternEdgeArrayList tmpEdgesAddedByPos = tmpEdges.get(i);
            PatternEdgeArrayList minEdgesAddedByPos = minEdges.get(i);

            patternEdgePool.reclaimObjects(minEdgesAddedByPos);
            minEdgesAddedByPos.clear();

            for (int j = 0; j < tmpEdgesAddedByPos.size(); ++j) {
                minEdgesAddedByPos.add(createPatternEdge(tmpEdgesAddedByPos.get(j)));
            }
        }

        minLabelling.clear();
        intIntAddConsumer.setMap(minLabelling);
        tmpLabelling.forEach(intIntAddConsumer);

        minInverseLabelling.clear();
        intIntAddConsumer.setMap(minInverseLabelling);
        tmpInverseLabelling.forEach(intIntAddConsumer);

        foundMinimum = true;
    }

    private void addTmpEdges(PatternEdgeArrayList edgesToAdd) {
        tmpEdges.add(edgesToAdd);
    }

    private void removeLastTmpEdges() {
        tmpEdges.remove(tmpEdges.size() - 1).reclaim();
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

        IntArrayList underlyingVertexPosThatExtendTmp = IntArrayListPool.instance().createObject();
        underlyingVertexPosThatExtendTmp.ensureCapacity(numUnderlyingVertices);

        for (int i = 0; i < numUnderlyingVertices; ++i) {
            if (!tmpLabelling.containsKey(i)) {
                underlyingVertexPosThatExtendTmp.add(i);
            }
        }

        return underlyingVertexPosThatExtendTmp;
    }

    private class AddCandidatePatternEdgeConsumer implements IntConsumer {
        private MainGraph mainGraph;
        private PatternEdgeArrayList candidateEdgesList;
        private int neighbourTmpPos, newTmpVertexPos, neighbourVertexId;

        public AddCandidatePatternEdgeConsumer() {
            mainGraph = Configuration.get().getMainGraph();
        }

        public void setCandidateEdgesList(PatternEdgeArrayList candidateEdges) {
            this.candidateEdgesList = candidateEdges;
        }

        public void setNeighbourTmpPos(int neighbourTmpPos) {
            this.neighbourTmpPos = neighbourTmpPos;
        }

        public void setNewTmpVertexPos(int newTmpVertexPos) {
            this.newTmpVertexPos = newTmpVertexPos;
        }

        public void setNeighbourVertexId(int neighbourVertexId) {
            this.neighbourVertexId = neighbourVertexId;
        }

        @Override
        public void accept(int edgeId) {
            Edge edge = mainGraph.getEdge(edgeId);

            candidateEdgesList.add(createPatternEdge(edge, neighbourTmpPos, newTmpVertexPos, neighbourVertexId));
        }
    }

}
