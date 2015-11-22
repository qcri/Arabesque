package io.arabesque.test;

import io.arabesque.conf.Configuration;
import io.arabesque.conf.TestConfiguration;
import io.arabesque.embedding.Embedding;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.BasicMainGraph;
import io.arabesque.graph.LabelledEdge;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

public class TestLabelledEdges {
    public static void main(String[] args) {
        org.apache.hadoop.conf.Configuration giraphConfiguration = new org.apache.hadoop.conf.Configuration();
        giraphConfiguration.setBoolean(Configuration.CONF_MAINGRAPH_EDGE_LABELLED, true);
        Configuration.setIfUnset(new TestConfiguration(new ImmutableClassesGiraphConfiguration(giraphConfiguration)));

        test("Straight line", new TestConstructor() {
            @Override
            public MainGraph constructMainGraph() {
                MainGraph mainGraph = new BasicMainGraph("test");

                mainGraph.addVertex(new Vertex(0, 1));
                mainGraph.addVertex(new Vertex(1, 1));
                mainGraph.addVertex(new Vertex(2, 1));
                mainGraph.addVertex(new Vertex(3, 1));

                mainGraph.addEdge(new LabelledEdge(0, 0, 1, 0));
                mainGraph.addEdge(new LabelledEdge(1, 1, 2, 1));
                mainGraph.addEdge(new LabelledEdge(2, 2, 3, 2));

                return mainGraph;
            }

            @Override
            public Embedding constructEmbedding() {
                VertexInducedEmbedding e = new VertexInducedEmbedding();
                e.addWord(0);
                e.addWord(1);
                e.addWord(2);
                e.addWord(3);

                return e;
            }
        });

        test("Star", new TestConstructor() {
            @Override
            public MainGraph constructMainGraph() {
                MainGraph mainGraph = new BasicMainGraph("test");

                mainGraph.addVertex(new Vertex(0, 1));
                mainGraph.addVertex(new Vertex(1, 1));
                mainGraph.addVertex(new Vertex(2, 1));
                mainGraph.addVertex(new Vertex(3, 1));
                mainGraph.addVertex(new Vertex(4, 1));

                mainGraph.addEdge(new LabelledEdge(0, 0, 1, 0));
                mainGraph.addEdge(new LabelledEdge(1, 0, 2, 1));
                mainGraph.addEdge(new LabelledEdge(2, 0, 3, 0));
                mainGraph.addEdge(new LabelledEdge(3, 0, 4, 1));

                return mainGraph;
            }

            @Override
            public Embedding constructEmbedding() {
                VertexInducedEmbedding e = new VertexInducedEmbedding();

                e.addWord(0);
                e.addWord(1);
                e.addWord(2);
                e.addWord(3);
                e.addWord(4);

                return e;
            }
        });
    }

    public static void test(String name, TestConstructor testConstructor) {
        System.out.println("\nTesting " + name);
        org.apache.hadoop.conf.Configuration giraphConfiguration = new org.apache.hadoop.conf.Configuration();
        giraphConfiguration.setBoolean(Configuration.CONF_MAINGRAPH_EDGE_LABELLED, true);
        Configuration.setIfUnset(new TestConfiguration(new ImmutableClassesGiraphConfiguration(giraphConfiguration)));

        MainGraph mainGraph = testConstructor.constructMainGraph();

        Configuration.get().setMainGraph(mainGraph);
        Configuration.get().initialize();

        Embedding e = testConstructor.constructEmbedding();

        Pattern pattern = e.getPattern();

        System.out.println("Pattern: " + pattern);
        System.out.println("Canonical labelling: " + pattern.getCanonicalLabeling());
        System.out.println("Vertex equivalences: " + pattern.getVertexPositionEquivalences());
    }

    public interface TestConstructor {
        MainGraph constructMainGraph();
        Embedding constructEmbedding();
    }
}
