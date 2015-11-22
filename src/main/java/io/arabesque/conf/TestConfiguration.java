package io.arabesque.conf;

import io.arabesque.embedding.Embedding;
import io.arabesque.graph.BasicMainGraph;
import io.arabesque.graph.MainGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

import java.io.IOException;
import java.nio.file.Paths;

public class TestConfiguration<O extends Embedding> extends Configuration<O> {
    public TestConfiguration(MainGraph mainGraph) {
        this(new ImmutableClassesGiraphConfiguration(new org.apache.hadoop.conf.Configuration()));
    }

    public TestConfiguration(ImmutableClassesGiraphConfiguration underlyingConfiguration) {
        super(underlyingConfiguration);
        getUnderlyingConfiguration().set(CONF_COMPUTATION_CLASS, "io.arabesque.examples.clique.CliqueComputation");
    }

    @Override
    public MainGraph createGraph() {
        try {
            return new BasicMainGraph(Paths.get(getMainGraphPath()), false, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
