package io.arabesque.pattern

import io.arabesque.conf.Configuration
import io.arabesque.embedding.Embedding
import io.arabesque.graph.Edge
import io.arabesque.graph.MainGraph
import io.arabesque.pattern.pool.PatternEdgePool
import io.arabesque.testutils.EmbeddingUtils
import io.arabesque.testutils.ListUtils
import io.arabesque.testutils.PatternUtils
import io.arabesque.testutils.graphs.LabelledTestGraph
import io.arabesque.testutils.graphs.TestGraph
import io.arabesque.testutils.graphs.UnlabelledTestGraph
import io.arabesque.utils.collection.IntArrayList
import net.openhft.koloboke.collect.map.IntIntMap
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration
import org.apache.giraph.utils.io.ExtendedDataInputOutput
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
abstract class PatternSpec extends Specification {
    @Shared static final int MAX_PERMUTATIONS = 5

    @Shared static final TestGraph TEST_GRAPH_UNLABELLED = new UnlabelledTestGraph()
    @Shared static final TestGraph TEST_GRAPH_LABELLED = new LabelledTestGraph()

    @Shared List<TestGraph> TEST_GRAPHS = createTestGraphs()
    @Shared Map<TestGraph, Map<List<Integer>, VertexPositionEquivalences>> GRAPH_TO_EMBEDDING_TO_EXPECTED_EQUIV = createGraphToEmbeddingToExpectedEquivMap()

    Configuration configuration

    def setup() {
        configuration = Mock(Configuration)
        Configuration.set(configuration)
    }

    def setMainGraph(MainGraph mainGraph) {
        configuration.getMainGraph() >> mainGraph
        configuration.isGraphEdgeLabelled() >> mainGraph.isEdgeLabelled()
        configuration.isGraphMulti() >> mainGraph.isMultiGraph()
        configuration.getUnderlyingConfiguration() >> new ImmutableClassesGiraphConfiguration(new GiraphConfiguration())
        Configuration.set(configuration)
    }

    /* WARNING:
     * All graphs in this list should have the same base structure (check
     * javadoc of createUnlabelledTestGraph) for the tests to work correctly.
     *
     * If you want to test things with a totally different graph feel free to do
     * so but don't add it to this collection.
     *
     * You may add new graphs which just differ in the labels.
     *
     * If you add extra vertices or edges these should have ids higher than
     * those present in the base graphs otherwise we risk constructing
     * disconnected embeddings and patters in the tests for which current behaviour
     * is undefined.
     */
    def List<TestGraph> createTestGraphs() {
        [
                TEST_GRAPH_UNLABELLED,
                TEST_GRAPH_LABELLED
        ]
    }

    def Map<TestGraph, Map<List<Integer>, VertexPositionEquivalences>> createGraphToEmbeddingToExpectedEquivMap() {
        createTestGraphs().collectEntries { graph -> [(graph): graph.getVEquivMap()] }
    }

    def "Default pattern should be empty"() {
        given: "default pattern"
        Pattern pattern = createPattern()
        expect: "pattern should be empty"
        checkPatternEmpty(pattern)
    }

    def "Copy of pattern should be equal to original"() {
        given: "a labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "a path pattern on that graph"
        Pattern pattern = createPatternFromVertexIds(TEST_GRAPH_LABELLED.EMBEDDING_PATH_VERTICES)

        when: "pattern is copied"
        def patternCopy = pattern.copy()

        then: "patterns should be equal"
        patternCopy == pattern
        then: "copy should not be a pointer to the original pattern"
        !patternCopy.is(pattern);
    }

    def "Modification of pattern copy should not change original"() {
        given: "a labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "a star pattern on that graph"
        Pattern pattern = createPatternFromVertexIds(TEST_GRAPH_LABELLED.EMBEDDING_STAR_VERTICES)

        when: "pattern is copied and modified"
        def patternCopy = pattern.copy()
        patternCopy.addEdge(5)

        then: "patterns should NOT be equal"
        patternCopy != pattern
    }

    def "Resetting a pattern should make it empty"() {
        given: "a labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "a star pattern on that graph"
        Pattern pattern = createPatternFromVertexIds(TEST_GRAPH_LABELLED.EMBEDDING_STAR_VERTICES)

        when: "pattern is reset"
        pattern.reset()

        then: "pattern should be empty"
        checkPatternEmpty(pattern)
    }

    def "Pattern and embedding created from vertices #vertexIds should have same structure [#mainGraph]"(
            MainGraph mainGraph, List<Integer> vertexIds) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()
        and: "a non-empty embedding created from vertex ids #vertexIds"
        Embedding embedding = EmbeddingUtils.createVertexEmbedding(vertexIds)

        when: "pattern is set from embedding"
        pattern.setEmbedding(embedding)

        then: "pattern has same structure as embedding"
        checkSameStructure(pattern, embedding)

        where: "we try all combinations of test graphs and test embeddings"
        [mainGraph, vertexIds] << TEST_GRAPHS.collectMany{ graph ->
            [graph, graph.getEmbeddingsVertices()].combinations()
        }
    }

    def "Pattern structure should remain correct throughout modifications of that pattern [#mainGraph]"(
            MainGraph mainGraph) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()
        and: "an associated embedding with which to compare"
        Embedding incrementalEmbedding = EmbeddingUtils.createEdgeEmbedding([])

        when: "we add new edge with id 1"
        incrementalEmbedding.addWord(1)
        pattern.addEdge(1)
        then: "pattern should continue having same structure as the associated embedding"
        checkSameStructure(pattern, incrementalEmbedding)

        when: "we add new edge with id 3"
        incrementalEmbedding.addWord(3)
        pattern.addEdge(3)
        then: "pattern should continue having same structure as the associated embedding"
        checkSameStructure(pattern, incrementalEmbedding)

        when: "we reset the pattern"
        incrementalEmbedding.removeLastWord()
        incrementalEmbedding.removeLastWord()
        pattern.reset()
        then: "pattern should be empty again"
        pattern.getNumberOfVertices() == 0
        pattern.getNumberOfEdges() == 0

        when: "we add new edge with id 0"
        incrementalEmbedding.addWord(0)
        pattern.addEdge(0)
        then: "pattern should have same structure as an embedding with a single edge with id 0"
        checkSameStructure(pattern, incrementalEmbedding)

        when: "we add new edge with id 1"
        incrementalEmbedding.addWord(1)
        pattern.addEdge(1)
        then: "pattern should continue having same structure as the associated embedding"
        checkSameStructure(pattern, incrementalEmbedding)

        when: "we add new edge with id 2"
        incrementalEmbedding.addWord(2)
        pattern.addEdge(2)
        then: "pattern should continue having same structure as the associated embedding"
        checkSameStructure(pattern, incrementalEmbedding)

        when: "we add new edge with id 3"
        incrementalEmbedding.addWord(3)
        pattern.addEdge(3)
        then: "pattern should continue having same structure as the associated embedding"
        checkSameStructure(pattern, incrementalEmbedding)

        where: "we try this with all test graphs"
        mainGraph << TEST_GRAPHS
    }

    def "Pattern should stay empty when setting from an empty embedding"() {
        given: "a labelled test graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "an empty pattern"
        Pattern pattern = createPattern()
        and: "and empty embedding"
        Embedding embedding = EmbeddingUtils.createVertexEmbedding([])

        when: "we set the pattern from the empty embedding"
        pattern.setEmbedding(embedding)
        then: "pattern should remain empty"
        checkPatternEmpty(pattern)
    }

    def "Pattern should be correct after consecutive 'from scratch' sets from vertex induced embeddings [#mainGraph]"(
            MainGraph mainGraph) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()

        when: "we set pattern from a vertex induced embedding"
        Embedding embedding1 = EmbeddingUtils.createVertexEmbedding([0, 1, 2, 3])
        pattern.setEmbedding(embedding1)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding1)

        when: "we set pattern from another very different vertex induced embedding"
        Embedding embedding2 = EmbeddingUtils.createVertexEmbedding([4, 5, 6])
        pattern.setEmbedding(embedding2)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding2)

        when: "we set pattern from another very different vertex induced embedding"
        Embedding embedding3 = EmbeddingUtils.createVertexEmbedding([6, 5, 4])
        pattern.setEmbedding(embedding3)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding3)

        when: "we set pattern from another very different vertex induced embedding"
        Embedding embedding4 = EmbeddingUtils.createVertexEmbedding([5, 3, 4, 0])
        pattern.setEmbedding(embedding4)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding4)

        where: "we try this with all test graphs"
        mainGraph << TEST_GRAPHS
    }

    def "Pattern should be correct after consecutive 'from scratch' sets from edge induced embeddings [#mainGraph]"(
            MainGraph mainGraph) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()

        when: "we set pattern from an edge induced embedding"
        Embedding embedding1 = EmbeddingUtils.createEdgeEmbedding([0, 1, 2, 3])
        pattern.setEmbedding(embedding1)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding1)

        when: "we set pattern from another very different edge induced embedding"
        Embedding embedding2 = EmbeddingUtils.createEdgeEmbedding([4, 5, 6])
        pattern.setEmbedding(embedding2)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding2)

        when: "we set pattern from another very different edge induced embedding"
        Embedding embedding3 = EmbeddingUtils.createEdgeEmbedding([6, 5, 4])
        pattern.setEmbedding(embedding3)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding3)

        when: "we set pattern from another very different edge induced embedding"
        Embedding embedding4 = EmbeddingUtils.createEdgeEmbedding([5, 3, 4, 0])
        pattern.setEmbedding(embedding4)
        then: "we should now have the same structure as that embedding"
        checkSameStructure(pattern, embedding4)

        where: "we try this with all test graphs"
        mainGraph << TEST_GRAPHS
    }

    def "Pattern should be correct after consecutive 'incremental' sets from vertex induced embeddings [#mainGraph]"(
            MainGraph mainGraph) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()

        when: "we set pattern from a vertex induced embedding"
        Embedding embedding1 = EmbeddingUtils.createVertexEmbedding([0, 4, 6, 1])
        pattern.setEmbedding(embedding1)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding1)

        when: "we set pattern from an embedding that differs from the previous in the last vertex"
        Embedding embedding2 = EmbeddingUtils.createVertexEmbedding([0, 4, 6, 2])
        pattern.setEmbedding(embedding2)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding2)

        when: "we set pattern from an embedding that differs from the previous in the last vertex"
        Embedding embedding3 = EmbeddingUtils.createVertexEmbedding([0, 4, 6, 5])
        pattern.setEmbedding(embedding3)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding3)

        when: "we set pattern from an embedding that differs from the previous in the last vertex"
        Embedding embedding4 = EmbeddingUtils.createVertexEmbedding([0, 4, 6, 3])
        pattern.setEmbedding(embedding4)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding4)

        where: "we try this with all test graphs"
        mainGraph << TEST_GRAPHS
    }

    def "Pattern should be correct after consecutive 'incremental' sets from edge induced embeddings [#mainGraph]"(
            MainGraph mainGraph) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()

        when: "we set pattern from an edge induced embedding"
        Embedding embedding1 = EmbeddingUtils.createEdgeEmbedding([7, 4, 2, 3])
        pattern.setEmbedding(embedding1)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding1)

        when: "we set pattern from an embedding that differs from the previous in the last edge"
        Embedding embedding2 = EmbeddingUtils.createEdgeEmbedding([7, 4, 2, 0])
        pattern.setEmbedding(embedding2)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding2)

        when: "we set pattern from an embedding that differs from the previous in the last edge"
        Embedding embedding3 = EmbeddingUtils.createEdgeEmbedding([7, 4, 2, 1])
        pattern.setEmbedding(embedding3)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding3)

        when: "we set pattern from an embedding that differs from the previous in the last edge"
        Embedding embedding4 = EmbeddingUtils.createEdgeEmbedding([7, 4, 2, 5])
        pattern.setEmbedding(embedding4)
        then: "we should have the same structure as that embedding"
        checkSameStructure(pattern, embedding4)

        where: "we try this with all test graphs"
        mainGraph << TEST_GRAPHS
    }

    def "Pattern from vertices #vertexIds should be consistent after read/write [#mainGraph]"(
            MainGraph mainGraph, List<Integer> vertexIds) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an embedding constructed from vertices #vertexIds"
        Embedding embedding = EmbeddingUtils.createVertexEmbedding(vertexIds)
        and: "a pattern constructed from that embedding"
        Pattern pattern = createPattern()
        pattern.setEmbedding(embedding)
        and: "a copy of this original pattern"
        Pattern originalPattern = pattern.copy();
        and: "a place to write the pattern to"
        ExtendedDataInputOutput dataInputOutput = new ExtendedDataInputOutput(Configuration.get().getUnderlyingConfiguration());

        when: "pattern is written and read again"
        pattern.write(dataInputOutput.getDataOutput())
        pattern.readFields(dataInputOutput.createDataInput())

        then: "read pattern is equal to original and has same structure as the embedding that created it"
        pattern == originalPattern
        checkSameStructure(pattern, embedding)

        where: "we try all combinations of test graphs and test embeddings"
        [mainGraph, vertexIds] << TEST_GRAPHS.collectMany{ graph ->
            [graph, graph.getEmbeddingsVertices()].combinations()
        }
    }

    /**
     * @param vertexIds Any valid (chain-connected) ordering of vertex ids to construct pattern from.
     * @param expectedEquivalences Expected equivalences assuming positions reflect numerical order of vertex ids.
     */
    def "Pattern from vertices #vertexIds should have the following vertex position equivalences #expectedEquivalences [#mainGraph]"(
            MainGraph mainGraph, List<Integer> vertexIds, VertexPositionEquivalences expectedEquivalences) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "a pattern constructed from vertices #vertexIds"
        Pattern pattern = createPatternFromVertexIds(vertexIds)

        when: "vertex position equivalences are calculated"
        VertexPositionEquivalences equivalences = pattern.getVertexPositionEquivalences()

        then: "they should match the expected equivalences"
        checkEquivalencesMatch(pattern, equivalences, expectedEquivalences)

        where: "we try all combinations of test embeddings and graphs"
        [mainGraph, vertexIds, expectedEquivalences] << TEST_GRAPHS.collectMany { it ->
            getRandomGraphVerticesExpectedEquivTripleCombinations(it, MAX_PERMUTATIONS)
        }
    }

    def "Pattern should have correct vertex position equivalences even when being reused [#mainGraph]"(
            MainGraph mainGraph, Map<List<Integer>, VertexPositionEquivalences> vertexIdsEquivsMap) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an empty pattern"
        Pattern pattern = createPattern()

        expect: "when reusable pattern set from embedding, equivalences should match expectations"
        vertexIdsEquivsMap.each { vertexIds, vEquivs ->
            Embedding embedding = EmbeddingUtils.createVertexEmbedding(vertexIds)
            pattern.setEmbedding(embedding)
            checkEquivalencesMatch(pattern, pattern.getVertexPositionEquivalences(), vEquivs)
        }

        where: "we reuse the same pattern with all embeddings of all test graphs"
        [mainGraph, vertexIdsEquivsMap] << TEST_GRAPHS.collect { graph -> [
                graph,
                GRAPH_TO_EMBEDDING_TO_EXPECTED_EQUIV.get(graph)
        ] }
    }

    def "There should only be 1 canonical pattern for all permutations of vertices #vertexIds [#mainGraph]"(
            MainGraph mainGraph, List<Integer> vertexIds) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "a storage for all canonical patterns found"
        Set<Pattern> canonicalPatterns = new HashSet<>()

        when: "we calculate canonical patterns for all valid permutations of vertices #vertexIds"
        getRandomValidVertexIdPermutations(mainGraph, vertexIds, MAX_PERMUTATIONS).each { permutedVertexIds ->
            Pattern pattern = createPatternFromVertexIds(permutedVertexIds)
            pattern.turnCanonical()
            canonicalPatterns.add(pattern)
        }

        then: "size of canonical pattern set should be 1 since all patterns over the same embedding should be isomorphic"
        canonicalPatterns.size() == 1

        where: "we test with all embeddings of all test graphs"
        [mainGraph, vertexIds] << TEST_GRAPHS.collectMany { graph -> [
                graph,
                GRAPH_TO_EMBEDDING_TO_EXPECTED_EQUIV.get(graph).keySet()
        ].combinations() }
    }

    def "There should only be 1 canonical pattern between 2 equivalent patterns over different vertex ids"() {
        given: "a single label main graph"
        setMainGraph(TEST_GRAPH_UNLABELLED)

        when: "we calculate canonical patterns for 2 different path embeddings with compatible label+structure properties"
        Pattern patternPath1 = createPatternFromVertexIds(TEST_GRAPH_UNLABELLED.EMBEDDING_PATH_VERTICES)
        patternPath1.turnCanonical()
        Pattern patternPath2 = createPatternFromVertexIds(TEST_GRAPH_UNLABELLED.EMBEDDING_PATH_ALT_VERTICES)
        patternPath2.turnCanonical()

        then: "those canonical patterns should be equal to one another"
        patternPath1 == patternPath2
    }

    def "We should be able to get string representation for pattern with vertices #vertexIds without any errors [#mainGraph]"(
            MainGraph mainGraph, List<Integer> vertexIds) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "a pattern constructed from vertices #vertexIds"
        Pattern pattern = createPatternFromVertexIds(vertexIds)

        expect: "string representation does not throw errors and has content (if pattern is not empty)"
        String repr = pattern.toString()
        if (pattern.getNumberOfVertices() > 0) {
            !repr.isEmpty()
            !repr.isAllWhitespace()
        }


        where: "we test with all embeddings of all test graphs"
        [mainGraph, vertexIds] << TEST_GRAPHS.collectMany{ graph ->
            [graph, graph.getEmbeddingsVertices()].combinations()
        }
    }

    void checkEquivalencesMatch(Pattern pattern, VertexPositionEquivalences equivalences, VertexPositionEquivalences expectedEquivalences) {
        IntIntMap vertexIdToPosMap = getVertexIdToPatternPosition(pattern)

        List<Integer> sortedVertices = new ArrayList(pattern.getVertices())
        sortedVertices.sort()

        IntIntMap vertexPosMappingToExpected = HashIntIntMaps.newMutableMap()

        int i = 0
        for (Integer vertexId : sortedVertices) {
            int oldPos = vertexIdToPosMap.get(vertexId)
            vertexPosMappingToExpected.put(oldPos, i)
            ++i
        }

        equivalences.convertBasedOnRelabelling(vertexPosMappingToExpected)

        assert equivalences == expectedEquivalences
    }

    void checkPatternEmpty(Pattern pattern) {
        PatternUtils.checkPatternEmpty(pattern)
    }

    void checkSameStructure(Pattern pattern, Embedding embedding) {
        assert pattern.getNumberOfVertices() == embedding.getNumVertices()
        assert pattern.getNumberOfEdges() == embedding.getNumEdges()

        IntArrayList embeddingVertices = embedding.getVertices()
        IntArrayList patternVertices = pattern.getVertices()

        assert patternVertices.containsAll(embeddingVertices)

        IntIntMap vertexToPositionMap = getVertexIdToPatternPosition(pattern)

        int[] embeddingEdges = embedding.getEdges()
        int numEmbeddingEdges = embedding.getNumEdges()

        PatternEdgeArrayList expectedEmbeddingPatternEdges = new PatternEdgeArrayList(numEmbeddingEdges)

        MainGraph mainGraph = Configuration.get().getMainGraph()

        for (int i = 0; i < numEmbeddingEdges; ++i) {
            int currentEdgeId = embeddingEdges[i]
            Edge edge = mainGraph.getEdge(currentEdgeId)

            PatternEdge patternEdge = getPatternEdgeFromEdgeAndMap(edge, vertexToPositionMap)

            expectedEmbeddingPatternEdges.add(patternEdge)
        }

        expectedEmbeddingPatternEdges.sort()

        PatternEdgeArrayList patternEdgesCopy = new PatternEdgeArrayList(pattern.getEdges())
        patternEdgesCopy.sort()

        assert expectedEmbeddingPatternEdges == patternEdgesCopy
    }

    void checkEdgeMatches(PatternEdge patternEdge, int edgeId, Pattern pattern) {
        MainGraph mainGraph = Configuration.get().getMainGraph();

        checkEdgeMatches(patternEdge, mainGraph.getEdge(edgeId), pattern);
    }

    void checkEdgeMatches(PatternEdge patternEdge, Edge actualEdge, Pattern pattern) {
        IntIntMap idToPosMap = getVertexIdToPatternPosition(pattern)

        PatternEdge patternEdgeFromActual = getPatternEdgeFromEdgeAndMap(actualEdge, idToPosMap)

        assert patternEdge == patternEdgeFromActual
    }

    IntIntMap getVertexIdToPatternPosition(Pattern pattern) {
        IntArrayList patternVertices = pattern.getVertices()
        IntIntMap vertexToPositionMap = HashIntIntMaps.newMutableMap()

        for (int i = 0; i < patternVertices.size; ++i) {
            vertexToPositionMap.put(patternVertices.getUnchecked(i), i)
        }

        return vertexToPositionMap
    }

    PatternEdge getPatternEdgeFromEdgeAndMap(Edge edge, IntIntMap vertexToPositionMap) {
        PatternEdge patternEdge = PatternEdgePool.instance().createObject()

        int srcPos = vertexToPositionMap.get(edge.getSourceId())
        int dstPos = vertexToPositionMap.get(edge.getDestinationId())

        patternEdge.setFromEdge(edge, srcPos, dstPos)

        // TODO: Remove when considering directed edges
        if (srcPos > dstPos) {
            patternEdge.invert()
        }

        return patternEdge
    }

    abstract Pattern createPattern();

    Pattern createPatternFromVertexIds(List<Integer> vertexIds) {
        Pattern pattern = createPattern()
        PatternUtils.resetPatternFromVertexIds(pattern, vertexIds)
        return pattern
    }

    List<List<Integer>> getRandomValidVertexIdPermutations(MainGraph mainGraph, List<Integer> vertexIds, int maxPermutations) {
        def validPermutations = EmbeddingUtils.getValidVertexIdPermutations(mainGraph, vertexIds)
        return ListUtils.getRandomSample(validPermutations, maxPermutations)
    }

    def getRandomGraphVerticesExpectedEquivTripleCombinations(MainGraph mainGraph, int numPermutationsPerEmbedding) {
        // For each (list<vertex ids>, vertex equivalences) pair for the provided maingraph
        // Construct a list of [mainGraph, list<vertex ids>, vertex equivalences] combinations
        // Then flatten all created lists for all pairs into a single list of triples (collectMany)
        return GRAPH_TO_EMBEDDING_TO_EXPECTED_EQUIV.get(mainGraph).collectMany { vertices, vequivs ->
            [
                    mainGraph,
                    getRandomValidVertexIdPermutations(mainGraph, vertices, numPermutationsPerEmbedding),
                    vequivs
            ].combinations()
        }
    }

}
