package io.arabesque.computation;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.aggregation.AggregationStorageFactory;
import io.arabesque.aggregation.AggregationStorageMetadata;
import io.arabesque.computation.comm.CommunicationStrategy;
import io.arabesque.computation.comm.MessageWrapper;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExecutionEngine<O extends Embedding>
        extends BasicComputation<IntWritable, NullWritable, NullWritable, MessageWrapper> 
        implements CommonExecutionEngine<O> {
    private static final Logger LOG = Logger.getLogger(ExecutionEngine.class);

    protected Configuration<O> configuration;
    protected WorkerContext workerContext;
    private CommunicationStrategy<O> communicationStrategy;

    private long numEmbeddingsProcessed;
    private long numEmbeddingsGenerated;

    private AggregationStorageFactory aggregationStorageFactory;
    private Map<String, AggregationStorage> aggregationStorages;
    private long numberOfEmbeddingsOutput;

    private int phase;
    private int numPhases;
    private long currentEmbeddingChildrenGenerated;
    private O currentEmbedding;
    private boolean firstVertex;

    private Computation<O> computation;

    public Computation<O> getComputation() {
        return computation;
    }

    public int getPhase() {
        return phase;
    }

    @Override
    public long getSuperstep() {
        return super.getSuperstep() / numPhases;
    }

    @Override
    public final void preSuperstep() {
        super.preSuperstep();
        init();
    }

    protected void init() {
        this.configuration = Configuration.get();
        this.aggregationStorageFactory = new AggregationStorageFactory();
        this.aggregationStorages = new HashMap<>();
        this.workerContext = getWorkerContext();
        this.communicationStrategy = configuration.createCommunicationStrategy(configuration,
                this, workerContext);
        this.numPhases = communicationStrategy.getNumPhases();

        phase = (int) super.getSuperstep() % numPhases;

        communicationStrategy.initialize(getPhase());

        numEmbeddingsProcessed = 0;
        numEmbeddingsGenerated = 0;
        numberOfEmbeddingsOutput = 0;

        firstVertex = true;

        if (getPhase() == 0) {
            computation = configuration.createComputation();
            computation.setUnderlyingExecutionEngine(this);

            if (getPhase() == 0 && getSuperstep() == 0) {
                if (configuration.getEmbeddingClass() == null) {
                    configuration.setEmbeddingClass(computation.getEmbeddingClass());
                }
            }

            computation.init();
        }
    }

    @Override
    public void postComputations() {
        communicationStrategy.finish();

        if (getPhase() == 0) {
            computation.finish();
        }
    }

    @Override
    public void output(String outputString) {
        workerContext.output(outputString);
        numberOfEmbeddingsOutput++;
    }

    @Override
    public void postSuperstep() {
        super.postSuperstep();

        try {
            for (Map.Entry<String, AggregationStorage> aggregationStorageEntry : aggregationStorages.entrySet()) {
                String aggregationStorageName = aggregationStorageEntry.getKey();
                AggregationStorage aggregationStorage = aggregationStorageEntry.getValue();

                workerContext.addAggregationStorage(aggregationStorageName, aggregationStorage);
            }
        } catch (RuntimeException e) {
            LOG.error(e);
            throw e;
        }

        LongWritable longWritable = new LongWritable();

        LOG.info("Num embeddings processed: " + numEmbeddingsProcessed);
        longWritable.set(numEmbeddingsProcessed);
        aggregate(MasterExecutionEngine.AGG_EMBEDDINGS_PROCESSED, longWritable);
        LOG.info("Num embeddings generated: " + numEmbeddingsGenerated);
        longWritable.set(numEmbeddingsGenerated);
        aggregate(MasterExecutionEngine.AGG_EMBEDDINGS_GENERATED, longWritable);
        LOG.info("Num embeddings output: " + numberOfEmbeddingsOutput);
        longWritable.set(numberOfEmbeddingsOutput);
        aggregate(MasterExecutionEngine.AGG_EMBEDDINGS_OUTPUT, longWritable);
    }

    @Override
    public void compute(Vertex<IntWritable, NullWritable, NullWritable> vertex,
            Iterable<MessageWrapper> receivedMessages) throws IOException {
        if (!firstVertex) {
            throw new RuntimeException("compute ran more than once in same SS. " +
                    "Check # of aggs. Has to be less than # of partitions");
        }

        communicationStrategy.startComputation(vertex, receivedMessages);

        if (getPhase() == 0) {
            expansionCompute(vertex);
        }

        communicationStrategy.endComputation();

        firstVertex = false;
    }

    protected void expansionCompute(Vertex<IntWritable, NullWritable, NullWritable> vertex) throws IOException {
        if (getSuperstep() == 0) {
            currentEmbeddingChildrenGenerated = 0L;

            bootstrap();

            numEmbeddingsGenerated += currentEmbeddingChildrenGenerated;
        } else {
            int currentDepth = 0;
            int numPendingProcessedEmbeddings = 0;

            while ((currentEmbedding = communicationStrategy.getNextInboundEmbedding()) != null) {
                currentDepth = currentEmbedding.getNumWords();

                currentEmbeddingChildrenGenerated = 0;

                internalCompute(currentEmbedding);

                numEmbeddingsGenerated += currentEmbeddingChildrenGenerated;
                ++numPendingProcessedEmbeddings;
                ++numEmbeddingsProcessed;

                if (numPendingProcessedEmbeddings % 100000 == 0) {
                    workerContext.reportProcessedInfo(currentDepth, numPendingProcessedEmbeddings);
                    numPendingProcessedEmbeddings = 0;
                }
            }

            if (numPendingProcessedEmbeddings > 0) {
                workerContext.reportProcessedInfo(currentDepth, numPendingProcessedEmbeddings);
            }
        }
    }

    protected void bootstrap() throws IOException {
        O embedding = configuration.createEmbedding();
        computation.expand(embedding);
    }

    protected void internalCompute(O currentObject)
            throws IOException {
        computation.expand(currentObject);
    }

    public void processExpansion(O expansion) {
        currentEmbeddingChildrenGenerated++;
        communicationStrategy.addOutboundEmbedding(expansion);
    }

    @Override
    public <A extends Writable> A getAggregatedValue(String name) {
        AggregationStorageMetadata metadata = Configuration.get().getAggregationMetadata(name);

        if (metadata == null) {
            return super.getAggregatedValue(name);
        } else {
            return workerContext.getAggregatedValue(name);
        }
    }

    public <K extends Writable, V extends Writable> void map(String name, K key, V value) {
        AggregationStorage<K, V> aggregationStorage = getAggregationStorage(name);

        aggregationStorage.aggregateWithReusables(key, value);
    }

    private <K extends Writable, V extends Writable> AggregationStorage<K, V> getAggregationStorage(String name) {
        AggregationStorage<K, V> aggregationStorage = aggregationStorages.get(name);

        if (aggregationStorage == null) {
            aggregationStorage = aggregationStorageFactory.createAggregationStorage(name);
            aggregationStorages.put(name, aggregationStorage);
        }

        return aggregationStorage;
    }

    public int getNumberPartitions() {
        return workerContext.getNumberPartitions();
    }

    @Override
    public void aggregate(String name, LongWritable value) {
       super.aggregate (name, value);
    }
}
