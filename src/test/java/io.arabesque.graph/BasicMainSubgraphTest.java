package io.arabesque.graph;

import io.arabesque.conf.SparkConfiguration;
import io.arabesque.conf.YamlConfiguration;
import io.arabesque.search.steps.QueryGraph;
import scala.collection.JavaConversions;

import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;


/**
 * Created by ghanemabdo on 4/12/18.
 */

public class BasicMainSubgraphTest {

    private final String queryGraphLocalPath = "src/main/resources/test-query-graph.graph";
    private final String graphLocalPath = "src/main/resources/test-graph.graph";
    private final String resultLocalPath = "src/main/resources/test-output/";
    private final String queryGraphHdfsPath = "hdfs:///input/qfrag/graph/test-query-graph.graph";
    private final String graphHdfsPath = "/input/qfrag/query/test-graph.graph";
    private final String resultHdfsPath = "/output/qfrag/test-output/";

    BasicMainGraph graph = null;
    QueryGraph query = null;
    SparkConfiguration config = null;

    @org.testng.annotations.BeforeMethod
    public void setUp() throws Exception {
        String[] params = {"/Users/ghanemabdo/Work/DS/qfrag/scripts/cluster-spark.yaml", "/Users/ghanemabdo/Work/DS/qfrag/scripts/search-configs.yaml"};
        YamlConfiguration yamlConfig = new YamlConfiguration(params);
        yamlConfig.load();

        config = new SparkConfiguration(JavaConversions.mapAsScalaMap(yamlConfig.getProperties()));
        config.sparkConf();
        //Configuration.set(config);

        graph = new BasicMainGraph(Paths.get(graphLocalPath));
        config.setMainGraph(graph);
    }

    @org.testng.annotations.AfterMethod
    public void tearDown() throws Exception {
    }

    @org.testng.annotations.Test
    public void testReadFromHdfs() throws Exception {

        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(resultHdfsPath);
        BasicMainSubgraph subgraph = new BasicMainSubgraph(hdfsPath);

        testSubgraphLoadedCorrectly(subgraph);
    }

    @org.testng.annotations.Test
    public void testReadFromFile() throws Exception {

        java.nio.file.Path path = Paths.get(resultLocalPath);
        BasicMainSubgraph subgraph = new BasicMainSubgraph(path);

        testSubgraphLoadedCorrectly(subgraph);
    }

    @org.testng.annotations.Test
    public void testGetOriginalVertex() throws Exception {

        BasicMainSubgraph subgraph = new BasicMainSubgraph(Paths.get(resultLocalPath));

        int vid = subgraph.getOriginalVertex(12);

        assertEquals(vid, 21, "Vertex mapping error");
    }

    @org.testng.annotations.Test
    public void testGetMappedVertex() throws Exception {
        BasicMainSubgraph subgraph = new BasicMainSubgraph(Paths.get(resultLocalPath));

        int mvid = subgraph.getMappedVertex(21).getVertexId();

        assertEquals(12, mvid, "Vertex mapping error");
    }

    private void testSubgraphLoadedCorrectly(BasicMainSubgraph subgraph) {
        assertEquals(subgraph.getNumberVertices(), 13, "Error in readFromFile");
        assertEquals(12, subgraph.getNumberEdges(), "Error in readFromFile");
    }
}