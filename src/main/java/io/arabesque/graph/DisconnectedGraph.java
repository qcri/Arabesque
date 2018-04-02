package io.arabesque.graph;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by ghanemabdo on 4/2/18.
 */
public class DisconnectedGraph extends BasicMainGraph {

    private String subgraphsName = null;
    protected HashMap<String, Integer> subgraphIdMap = new HashMap<>();
    protected HashMap<Integer, Integer> vertexSubgraphIdMap = new HashMap<>();

    private void init(Object path, Object subgraphsPath) throws IOException {
        long start = 0;

        if (LOG.isInfoEnabled()) {
            LOG.info("Reading graph");
            start = System.currentTimeMillis();
        }

        if (path instanceof Path && subgraphsPath instanceof Path) {
            Path filePath = (Path) path;
            Path subgraphsFilePath = (Path) subgraphsPath;
            readFromFile(filePath, subgraphsFilePath);
        } else if (path instanceof org.apache.hadoop.fs.Path) {
            org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
            org.apache.hadoop.fs.Path hadoopSubgraphsPath = (org.apache.hadoop.fs.Path) subgraphsPath;
            readFromHdfs(hadoopPath, hadoopSubgraphsPath);
        } else {
            throw new RuntimeException("Invalid path: " + path);
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Done in " + (System.currentTimeMillis() - start));
            LOG.info("Number vertices: " + numVertices);
            LOG.info("Number edges: " + numEdges);
        }
    }

    public DisconnectedGraph(String name, String subgraphsName, boolean isEdgeLabelled, boolean isMultiGraph) {
        super(name, isEdgeLabelled, isMultiGraph);
        this.subgraphsName = subgraphsName;
    }

    public DisconnectedGraph(String name, String subgraphsName) {
        this(name, subgraphsName, false, false);
    }

    public DisconnectedGraph(Path filePath, Path subgraphsfilePath, boolean isEdgeLabelled, boolean isMultiGraph)
            throws IOException {
        this(filePath.getFileName().toString(), subgraphsfilePath.getFileName().toString(), isEdgeLabelled, isMultiGraph);
        init(filePath, subgraphsfilePath);
    }

    public DisconnectedGraph(org.apache.hadoop.fs.Path hdfsPath, org.apache.hadoop.fs.Path hdfsSubgraphsPath, boolean isEdgeLabelled, boolean isMultiGraph)
            throws IOException {
        this(hdfsPath.getName(), hdfsSubgraphsPath.getName(), isEdgeLabelled, isMultiGraph);
        init(hdfsPath, hdfsSubgraphsPath);
    }

    protected void readFromHdfs(org.apache.hadoop.fs.Path hdfsPath, org.apache.hadoop.fs.Path hdfsSubgraphsPath) throws IOException {
        super.readFromHdfs(hdfsPath);

        FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
        InputStream sgis = fs.open(hdfsSubgraphsPath);
        parseSubgraphFile(sgis);
        sgis.close();

        this.addSubgraphLabelsToVertices();
    }

    protected void readFromFile(Path filePath, Path subgraphsFilePath) throws IOException {
        super.readFromFile(filePath);

        InputStream sgis = Files.newInputStream(subgraphsFilePath);
        parseSubgraphFile(sgis);
        sgis.close();

        this.addSubgraphLabelsToVertices();
    }

    private void parseSubgraphFile(InputStream sgis) {
        try {
            HashSet<String> subgraphNames = new HashSet<>();
            int subgraphsCounter = 0;

            BufferedReader reader = new BufferedReader(new InputStreamReader(new BOMInputStream(sgis)));

            String line = reader.readLine();

            while(line != null) {
                String[] lineComponents = line.split(" ");
                if(lineComponents.length > 0) {
                    int vertexId = Integer.parseInt(lineComponents[0]);
                    String graphId = lineComponents[lineComponents.length - 1];

                    if(!subgraphNames.contains(graphId)) {
                        subgraphNames.add(graphId);
                        subgraphIdMap.put(graphId, subgraphsCounter++);
                    }

                    vertexSubgraphIdMap.put(vertexId, subgraphIdMap.get(graphId));
                }

                line = reader.readLine();
            }
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void addSubgraphLabelsToVertices() {
        try {
            for (int i = 0 ; i < this.getNumberVertices() ; i++) {
                Vertex v = this.getVertex(i);
                Integer vertexSubgraph = vertexSubgraphIdMap.get(v.getVertexId());

                if (vertexSubgraph != null)
                    v.setSubgraphId(vertexSubgraph);
            }
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }
}
