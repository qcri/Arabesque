package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import jdk.nashorn.internal.objects.annotations.Getter;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ghanemabdo on 4/10/18.
 *
 * BasicMainSubgraph is used to store the output subgraph from QFrag and pass it over to Arabesque
 * for further processing. It reads QFrag's output embeddings from HDFS or the local file system
 * and constructs the corresponding subgraph as Arabesque's graph object
 */

public class BasicMainSubgraph extends BasicMainGraph {

    private final static Logger LOG = Logger.getLogger(BasicMainSubgraph.class);

    private HashMap<Integer, IntSet> adjMap;                //Adjacency list created from the embeddings
    private HashMap<Integer, Integer> vertexForwardMap;     //Mapping vertices in the original graph to sequential
                                                            //embedding in the subgraph
    private HashMap<Integer, Integer> vertexReverseMap;     //reverse mapping from subgraph vertex space to original
                                                            //graph space
    private int embeddingSize;                                  //the size of the query graph
    private Configuration conf;

    public BasicMainSubgraph() {}

    public BasicMainSubgraph(String name) {
        super(name);
    }

    /***
     * Construct subgraph given path to local folder containing one or more files.
     * each file has embeddings that represents part of the graph. Merging the embeddings, given its structure from
     * the original graph provide sufficient information to build the vertex induced sugraph
     * @param filePath path to local folder that contains the subgraph segments
     * @throws IOException  If the file does not exist or user does not have read permission
     */
    public BasicMainSubgraph(Path filePath) throws IOException {
        super(filePath);
        constructGraph();
    }

    /***
     * Construct subgraph given path to hdfs directory containing one or more files.
     * each file has embeddings that represents part of the graph. Merging the embeddings, given its structure from
     * the original graph provide sufficient information to build the vertex induced sugraph
     * @param hdfsPath path to hdfs directory that contains the subgraph segments
     * @throws IOException  If the path does not exist or user does not have read permission
     */
    public BasicMainSubgraph(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        super(hdfsPath);
        constructGraph();
    }

    /***
     * Overrides parent class's init method to initialize the data structures used to construct the subgraph (adjacency
     * list, vertices mapping & configuration). Then, it calls the methods that does the subgraph construction.
     * @param path path object to the folder that contains the embeddings segments. This can be local or on hdfs
     * @throws IOException If the path does not exist or user does not have read permission
     */
    @Override
    protected void init(Object path) throws IOException {
        adjMap = new HashMap<>();
        vertexForwardMap = new HashMap<>();
        vertexReverseMap = new HashMap<>();
        conf = Configuration.get();

        super.init(path);
    }

    /***
     * Read subgraph data from a path on hdfs. If path is a directory, it recurses over the files within it. Otherwise,
     * files are opened as stream and passed to another method for data extraction.
     * @param hdfsPath path on hdfs that could be directory or file.
     * @throws IOException If the path does not exist or user does not have read permission
     */
    @Override
    protected void readFromHdfs(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
        org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
        //hdfsConf.set("fs.defaultFS", "hdfs://localhost/");

        FileSystem fs = FileSystem.get(hdfsConf);

        // recursively loop over paths in the root directory
        // for directories, recurse to find the files
        // for files, open the input stream and parse the file to extract the embeddings
        if(fs.isDirectory(hdfsPath)) {
            for(FileStatus status : fs.listStatus(hdfsPath)) {
                readFromHdfs(status.getPath());
            }
        } else {
            InputStream is = null;

            try {
                is = fs.open(hdfsPath);
                readFromInputStream(is);
            } catch (IOException ex) {
                LOG.error("cannot open subgraph file " + hdfsPath, ex);
            }
            finally {
                is.close();
            }
        }
    }

    /***
     * Read subgraph data from a local path. If path is a directory, it recurses over the files within it. Otherwise,
     * files are opened as input stream and passed to another method for data extraction.
     * @param filePath local path that could be directory or file.
     * @throws IOException If the path does not exist or user does not have read permission
     */
    @Override
    protected void readFromFile(Path filePath) throws IOException {

        File f = filePath.toFile();

        // recursively loop over paths in the root directory
        // for directories, recurse to find the files
        // for files, open the input stream and parse the file to extract the embeddings
        if(f.isDirectory()) {
            for(File file : f.listFiles()) {
                readFromFile(file.toPath());
            }
        } else if(!f.getName().startsWith(".")) {
            InputStream is = null;

            try {
                is = Files.newInputStream(filePath);
                readFromInputStream(is);
            } catch (IOException ex) {
                LOG.error("cannot open subgraph file " + filePath, ex);
            }
            finally {
                is.close();
            }
        }
    }

    /***
     * Given an input stream, this method determines whether to read data in text or in binary based on values read
     * from the configuration
     * @param is the input stream to read data from
     * @throws IOException If there is a problem reading from the input stream
     */
    @Override
    protected void readFromInputStream(InputStream is) throws IOException {

        if (isBinary && isMultiGraph) {
            readFromInputStreamBinary(is);
        } else {
            readFromInputStreamText(is);
        }
    }

    /***
     * Read embeddings data as text from the passed input stream
     * @param is input stream that has embeddings as textual data
     */
    @Override
    protected void readFromInputStreamText(InputStream is) {

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new BOMInputStream(is)));

            String line = reader.readLine();

            while(line != null) {
                if(!Character.isDigit(line.charAt(0))) {
                    line = reader.readLine();
                    continue;
                }

                String[] splitted = line.split(" ");

                int[] vertices = new int[splitted.length];

                try {
                    for(int i = 0 ; i < splitted.length ; i++) {
                        vertices[i] = Integer.parseInt(splitted[i]);
                    }
                } catch (NumberFormatException ex) {
                    LOG.error("Cannot cast vertices within embedding " + line + " to integers. Dropping this embedding");
                    break;
                }

                fillInAdjTable(extractEdges(vertices));

                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            LOG.error("Cannot read from input file " + e.getLocalizedMessage());
        }
    }

    /***
     * Read embeddings as binary data from the passed input stream
     * @param is input stream that has embeddings data as binary values
     * @throws IOException If can not read from the input stream
     */
    @Override
    protected void readFromInputStreamBinary(InputStream is) throws IOException {

        this.getQuerySize();    //important to fill embeddingSize before reading the embeddings and constructing the graph

        BufferedInputStream a_ = null;
        DataInputStream in = null;

        try {
            a_ = new BufferedInputStream(is);
            in = new DataInputStream(a_);

            int[] vertices = new int[embeddingSize];

            while(in.available() > 0) {
                for(int i = 0 ; i < embeddingSize ; i++) {
                    vertices[i] = in.readInt();
                }

                fillInAdjTable(extractEdges(vertices));
            }
        } catch (IOException ex) {
            LOG.error("Cannot read from embeddings input stream");
            throw new IOException(ex);
        }
        finally {
            in.close();
            a_.close();
        }
    }

    /***
     * Given embeddings vertices, this method extracts the corresponding vertex induced edges from the original graph
     * @param vertices array of vertices represents one embedding match for the query graph
     * @return ArrayList of the edges of corresponding vertex induced subgraph
     */
    private ArrayList<Integer> extractEdges(int[] vertices) {
        MainGraph graph = conf.getMainGraph();
        ArrayList<Integer> edges = new ArrayList<>();

        for(int i = 0 ; i < vertices.length ; i++) {
            IntSet neighbours = HashIntSets.newImmutableSet(graph.getVertexNeighbours(vertices[i]));

            for(int j = i+1 ; j < vertices.length ; j++) {
                if( neighbours.contains(vertices[j])) {
                    edges.add(vertices[i]);
                    edges.add(vertices[j]);
                }
            }
        }

        return edges;
    }

    /***
     * filling the edges in the adjacency list
     * @param edges array of edges to feed into the adj list
     */
    private void fillInAdjTable(ArrayList<Integer> edges) {
        for(int i = 0 ; i < edges.size() ; i += 2) {
            addEdge(edges.get(i), edges.get(i+1));
        }
    }

    /***
     * Connects an edges between source vertex and destination vertex in the graph's adj list. The graph is assumed to
     * be undirected. So, the edge is also connected from destination to source
     * @param source source vertex id
     * @param dest destination vertex id
     */
    private void addEdge(int source, int dest) {
        IntSet neighbors = adjMap.get(source);

        if(neighbors == null) {
            neighbors = HashIntSets.newMutableSet();
            adjMap.put(source, neighbors);
        }

        neighbors.add(dest);

        neighbors = adjMap.get(dest);

        if(neighbors == null) {
            neighbors = HashIntSets.newMutableSet();
            adjMap.put(dest, neighbors);
        }

        neighbors.add(source);
    }

    /***
     * Given the adjacency list, the subgraph is constructed in a form of Arabesque Graph to be used later by Arabesque
     * for further processing.
     * Since the order of vertices in an Arabesque graph has to be sequential, the reconstructed subgraph vertices are
     * mapped to sequential values to be valid for use by arabesque. The forward and reverse mapping is preserved in
     * two dictionaries vertexForwardMap & vertexReverseMap
     */
    private void constructGraph() {
        int mappedVertexId = 0;

        for(int v : adjMap.keySet()) {
            vertexForwardMap.put(v, mappedVertexId);
            vertexReverseMap.put(mappedVertexId, v);
            int vertexLbl = conf.getMainGraph().getVertexLabel(v);
            addVertex(createVertex(mappedVertexId, vertexLbl));

            mappedVertexId++;
        }

        for(int src : adjMap.keySet()) {
            for(int dst : adjMap.get(src)) {
                int mappedDst = vertexForwardMap.get(dst);
                int mappedSrc = vertexForwardMap.get(src);

                if(mappedSrc < mappedDst) {
                    addEdge(createEdge(mappedSrc, mappedDst));
                }
            }
        }
    }

    /***
     * Calculates the output embeddings size from the query graph
     */
    private void getQuerySize () {

        String qPath = conf.getString(Configuration.SEARCH_QUERY_GRAPH_PATH, Configuration.SEARCH_QUERY_GRAPH_PATH_DEFAULT);
        InputStream is = null;

        try {
            is = new FileInputStream(qPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(new BOMInputStream(is)));

            String line = reader.readLine();

            while(line != null) {
                embeddingSize++;
                line = reader.readLine();
            }

            reader.close();
            is.close();
        } catch (IOException e) {
            LOG.error("Cannot read from query file " + e.getLocalizedMessage());
        }
    }

    /***
     * Given a mapped vertex id, the original vertex id from the orignal graph is retrieved
     * @param vid mapped vertex id
     * @return the original vertex id in the original graph
     */
    @Getter
    public int getOriginalVertex(int vid) {
        return vertexReverseMap.get(vid);
    }

    @Getter
    public int getOriginalVertex(Vertex v) {
        return getOriginalVertex(v.getVertexId());
    }

    @Getter
    public Vertex getMappedVertex(int vid) {
        Integer v = vertexForwardMap.get(vid);

        if(v != null) {
            return this.getVertex(v);
        }

        return null;
    }
}
