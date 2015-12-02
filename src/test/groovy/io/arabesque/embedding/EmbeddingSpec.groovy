package io.arabesque.embedding

import io.arabesque.conf.Configuration
import io.arabesque.graph.MainGraph
import io.arabesque.pattern.JBlissPattern
import io.arabesque.pattern.Pattern
import io.arabesque.pattern.VICPattern
import io.arabesque.testutils.EmbeddingUtils
import io.arabesque.testutils.PatternUtils
import io.arabesque.testutils.graphs.EdgeLabelledMultiTestGraph
import io.arabesque.testutils.graphs.LabelledTestGraph
import io.arabesque.testutils.graphs.TestGraph
import io.arabesque.testutils.graphs.TestGraph.EmbeddingId
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration
import org.apache.giraph.utils.io.ExtendedDataInputOutput
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
abstract class EmbeddingSpec extends Specification {
    @Shared static final TestGraph TEST_GRAPH_LABELLED = new LabelledTestGraph()
    @Shared static final TestGraph TEST_GRAPH_MULTI = new EdgeLabelledMultiTestGraph()

    @Shared List<TestGraph> TEST_GRAPHS = [
            TEST_GRAPH_LABELLED,
            TEST_GRAPH_MULTI
    ]

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
        configuration.createPattern() >> {
            if (mainGraph.isMultiGraph() || mainGraph.isEdgeLabelled()) {
                return new VICPattern()
            }
            else {
                return new JBlissPattern()
            }
        }
        Configuration.set(configuration)
    }

    def "Default embedding should be empty"() {
        given: "a graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "default embedding"
        Embedding embedding = createEmbedding()
        expect: "embedding should be empty"
        EmbeddingUtils.checkEmbeddingEmpty(embedding)
    }

    /*abstract def "Embedding structure should remain correct throughout modifications of that embedding in a simple graph"();
    abstract def "Embedding structure should remain correct throughout modifications of that embedding in a multi graph"();*/

    def "Embedding's pattern should match expected pattern for that structure [#mainGraph, #embeddingId]"(TestGraph mainGraph, EmbeddingId embeddingId) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an embedding constructed from the words associated with embedding #embeddingId"
        Embedding embedding = createEmbeddingFromId(mainGraph, embeddingId)
        and: "a pattern constructed from the same words"
        Pattern pattern = createPatternFromId(mainGraph, embeddingId)

        expect: "embedding's pattern and pattern's pattern should be exactly the same"
        embedding.getPattern() == pattern

        where: "we try all test graphs with their associated embedding ids"
        [mainGraph, embeddingId] << TEST_GRAPHS.collectMany { graph ->
            [graph, getGraphEmbeddingIds(graph)].combinations()
        }
    }

    def "Pattern of reusable embedding should be valid throughout modifications of that embedding"() {
        given: "a labelled graph"
        setMainGraph(TEST_GRAPH_LABELLED)
        and: "an initially empty embedding"
        Embedding embedding = createEmbedding()
        and: "a list with the word ids added to the embedding"
        List<Integer> wordIdsAdded = []

        when: "we add word with id 0"
        wordIdsAdded.add(0)
        embedding.addWord(0)
        and: "construct a pattern from the collection wordIdsAdded"
        Pattern pattern0 = createPatternFromWordIds(wordIdsAdded)

        then: "embedding's pattern should be equal to constructed pattern"
        embedding.getPattern() == pattern0

        when: "we add word with id 1"
        wordIdsAdded.add(1)
        embedding.addWord(1)
        and: "construct a pattern from the collection wordIdsAdded"
        Pattern pattern1 = createPatternFromWordIds(wordIdsAdded)

        then: "embedding's pattern should be equal to constructed pattern"
        embedding.getPattern() == pattern1

        when: "we add word with id 2"
        wordIdsAdded.add(2)
        embedding.addWord(2)
        and: "construct a pattern from the collection wordIdsAdded"
        Pattern pattern2 = createPatternFromWordIds(wordIdsAdded)

        then: "embedding's pattern should be equal to constructed pattern"
        embedding.getPattern() == pattern2

        when: "we add word with id 3"
        wordIdsAdded.add(3)
        embedding.addWord(3)
        and: "construct a pattern from the collection wordIdsAdded"
        Pattern pattern3 = createPatternFromWordIds(wordIdsAdded)

        then: "embedding's pattern should be equal to constructed pattern"
        embedding.getPattern() == pattern3

        when: "we add word with id 4"
        wordIdsAdded.add(4)
        embedding.addWord(4)
        and: "construct a pattern from the collection wordIdsAdded"
        Pattern pattern4 = createPatternFromWordIds(wordIdsAdded)

        then: "embedding's pattern should be equal to constructed pattern"
        embedding.getPattern() == pattern4

        when: "we remove word with id 4"
        wordIdsAdded.pop()
        embedding.removeLastWord()

        then: "embedding's pattern should be equal to constructed pattern before last addition"
        embedding.getPattern() == pattern3

        when: "we add word with id 5"
        wordIdsAdded.add(5)
        embedding.addWord(5)
        and: "construct a pattern from the collection wordIdsAdded"
        Pattern pattern5 = createPatternFromWordIds(wordIdsAdded)

        then: "embedding's pattern should be equal to constructed pattern"
        embedding.getPattern() == pattern5

        when: "we remove all words"
        int numWordsToRemove = wordIdsAdded.size();

        for (int i = 0; i < numWordsToRemove; ++i) {
            wordIdsAdded.pop()
            embedding.removeLastWord()
        }
        and: "construct an empty pattern"
        Pattern pattern6 = PatternUtils.createPattern()

        then: "embedding's pattern should be equal to empty pattern"
        embedding.getPattern() == pattern6
    }

    /*abstract def "Embedding extensions should remain correct throughout modifications of that embedding in a simple graph"();
    abstract def "Embedding extensions should remain correct throughout modifications of that embedding in a multi graph"();*/

    def "There should only be 1 canonical embedding for any valid permutation of word ids corresponding to #embeddingId [#mainGraph]"(TestGraph mainGraph, EmbeddingId embeddingId) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)

        when: "we attempt to construct embeddings with all valid permutations of word ids corresponding to #embeddingId"
        Set<Embedding> canonicalEmbeddings = new HashSet<>()
        List<Integer> wordIds = getWordIdsFromEmbeddingId(mainGraph, embeddingId)

        int i = 0;

        for (List<Integer> wordIdsPermutation : getValidWordIdsPermutations(mainGraph, wordIds)) {
            Embedding embedding = createCanonicalEmbeddingFromWordIds(wordIdsPermutation)

            if (embedding != null) {
                canonicalEmbeddings.add(embedding)
            }

            ++i;
        }

        then: "we expect that only 1 such embedding was deemed canonical"
        canonicalEmbeddings.size() == 1

        where: "we try all test graphs with their associated embedding ids (except for complete because there are too many permutations)"
        [mainGraph, embeddingId] << TEST_GRAPHS.collectMany { graph ->
            [graph, getGraphEmbeddingIds(graph) - EmbeddingId.COMPLETE].combinations()
        }
    }

    def "Embedding with id #embeddingId should be consistent after read/write [#mainGraph]"(
            TestGraph mainGraph, EmbeddingId embeddingId) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an embedding constructed from id #embeddingId"
        Embedding embedding = createEmbeddingFromId(mainGraph, embeddingId)
        and: "a copy of this original embedding"
        Embedding originalEmbedding = createEmbeddingFromId(mainGraph, embeddingId)
        and: "a place to write the embedding to"
        ExtendedDataInputOutput dataInputOutput = new ExtendedDataInputOutput(Configuration.get().getUnderlyingConfiguration());

        when: "embedding is written and read again"
        embedding.write(dataInputOutput.getDataOutput())
        embedding.readFields(dataInputOutput.createDataInput())

        then: "read embedding is equal to original"
        embedding == originalEmbedding

        where: "we try all combinations of test graphs and test embeddings"
        [mainGraph, embeddingId] << TEST_GRAPHS.collectMany{ graph ->
            [graph, getGraphEmbeddingIds(graph)].combinations()
        }
    }

    def "We should be able to get string representation for embeddings #embeddingId without any errors [#mainGraph]"(
            TestGraph mainGraph, EmbeddingId embeddingId) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an embedding corresponding to #embeddingId"
        Embedding embedding = createEmbeddingFromId(mainGraph, embeddingId)

        expect: "string representation does not throw errors and has content (if embedding is not empty)"
        String repr = embedding.toString()
        if (embedding.getNumWords() > 0) {
            !repr.isEmpty()
            !repr.isAllWhitespace()
        }

        where: "we test with all embeddings of all test graphs"
        [mainGraph, embeddingId] << TEST_GRAPHS.collectMany{ graph ->
            [graph, getGraphEmbeddingIds(graph)].combinations()
        }
    }

    def "We should be able to get an output string representation for embeddings #embeddingId without any errors [#mainGraph]"(
            TestGraph mainGraph, EmbeddingId embeddingId) {
        given: "a graph (#mainGraph)"
        setMainGraph(mainGraph)
        and: "an embedding corresponding to #embeddingId"
        Embedding embedding = createEmbeddingFromId(mainGraph, embeddingId)

        expect: "string representation does not throw errors and has content (if embedding is not empty)"
        String repr = embedding.toOutputString()
        if (embedding.getNumWords() > 0) {
            !repr.isEmpty()
            !repr.isAllWhitespace()
        }

        where: "we test with all embeddings of all test graphs"
        [mainGraph, embeddingId] << TEST_GRAPHS.collectMany{ graph ->
            [graph, getGraphEmbeddingIds(graph)].combinations()
        }
    }

    abstract Embedding createEmbedding()

    Embedding createEmbeddingFromWordIds(List<Integer> wordIds) {
        Embedding embedding = createEmbedding()

        for (Integer wordId : wordIds) {
            embedding.addWord(wordId)
        }

        return embedding
    }

    Embedding createCanonicalEmbeddingFromWordIds(List<Integer> wordIds) {
        Embedding embedding = createEmbedding()

        for (Integer wordId : wordIds) {
            if (!embedding.isCanonicalEmbeddingWithWord(wordId)) {
                return null;
            }

            embedding.addWord(wordId)
        }

        return embedding
    }

    abstract List<Integer> getWordIdsFromEmbeddingId(TestGraph graph, EmbeddingId embeddingId)
    abstract List<List<Integer>> getValidWordIdsPermutations(TestGraph graph, List<Integer> wordIds)

    Embedding createEmbeddingFromId(TestGraph graph, EmbeddingId embeddingId) {
        return createEmbeddingFromWordIds(getWordIdsFromEmbeddingId(graph, embeddingId))
    }

    Pattern createPatternFromId(TestGraph graph, EmbeddingId embeddingId) {
        Pattern pattern = PatternUtils.createPattern()
        pattern.setEmbedding(createEmbeddingFromId(graph, embeddingId))

        return pattern
    }

    Pattern createPatternFromWordIds(List<Integer> wordIds) {
        Pattern pattern = PatternUtils.createPattern()
        pattern.setEmbedding(createEmbeddingFromWordIds(wordIds))
        return pattern
    }

    abstract List<EmbeddingId> getGraphEmbeddingIds(TestGraph graph);
}
