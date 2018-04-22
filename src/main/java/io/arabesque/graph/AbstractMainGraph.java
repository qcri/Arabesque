package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractMainGraph implements MainGraph {
  private static final Logger LOG = Logger.getLogger(AbstractMainGraph.class);

  protected long numVertices; // no default
  protected long numEdges; // no default

  protected boolean isEdgeLabelled; // false
  protected boolean isFloatLabel; // false
  protected boolean isMultiGraph; // false
  protected boolean isBinary; // false

  protected String name;

  public AbstractMainGraph() {}

  public AbstractMainGraph(String name) {
    init(name);
  }

  // ONLY FOR TESTING TO AVOID CHANGING THE TEST...
  public AbstractMainGraph(String name, boolean a, boolean b) {
    init(name, a, b);
  }

  public AbstractMainGraph(Path filePath)
      throws IOException {
    this(filePath.getFileName().toString());
    init(filePath);
  }

  public AbstractMainGraph(org.apache.hadoop.fs.Path hdfsPath)
      throws IOException {
    this(hdfsPath.getName());
    init(hdfsPath);
  }


  // ONLY USED FOR TESTING TO AVOID CHANGING THE TESTS!!
  protected void init(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
    long start = 0;

    if (LOG.isInfoEnabled()) {
      start = System.currentTimeMillis();
      LOG.info("Initializing");
    }

    this.isEdgeLabelled = isEdgeLabelled;
    this.isMultiGraph = isMultiGraph;
    this.name = name;
    reset();

    if (LOG.isInfoEnabled()) {
      LOG.info("Done in " + (System.currentTimeMillis() - start));
    }
  }

  protected void init(String name) {//boolean isEdgeLabelled, boolean isMultiGraph) {
    long start = 0;

    if (LOG.isInfoEnabled()) {
      start = System.currentTimeMillis();
      LOG.info("Initializing");
    }

    Configuration conf = Configuration.get();
    isEdgeLabelled = conf.isGraphEdgeLabelled();
    isMultiGraph = conf.isGraphMulti();
    isFloatLabel = conf.isFloatEdge();
    isBinary = conf.isBinaryInputFile();
    this.name = name;
    reset();

    if (LOG.isInfoEnabled()) {
      LOG.info("Done in " + (System.currentTimeMillis() - start));
    }
  }

  protected void init(Object path) throws IOException {
    long start = 0;

    if (LOG.isInfoEnabled()) {
      start = System.currentTimeMillis();
      LOG.info("Initializing");
    }

    Configuration conf = Configuration.get();
    isEdgeLabelled = conf.isGraphEdgeLabelled();
    isMultiGraph = conf.isGraphMulti();
    isFloatLabel = conf.isFloatEdge();

    if (path instanceof Path) {
      Path filePath = (Path) path;
      readFromFile(filePath);
      System.gc();
    } else if (path instanceof org.apache.hadoop.fs.Path) {
      org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
      readFromHdfs(hadoopPath);
      System.gc();
    } else {
      throw new RuntimeException("Invalid path: " + path);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Done in " + (System.currentTimeMillis() - start));
      LOG.info("Number vertices: " + numVertices);
      LOG.info("Number edges: " + numEdges);
    }
  }

  protected void init(Object path, boolean isEdgeLabelled, boolean isMultiGraph) throws IOException {
    long start = 0;

    if (LOG.isInfoEnabled()) {
      start = System.currentTimeMillis();
      LOG.info("Initializing");
    }

    Configuration conf = Configuration.get();
    isFloatLabel = conf.isFloatEdge();
    this.isEdgeLabelled = isEdgeLabelled;
    this.isMultiGraph = isMultiGraph;

    if (path instanceof Path) {
      Path filePath = (Path) path;
      readFromFile(filePath);
      System.gc();
    } else if (path instanceof org.apache.hadoop.fs.Path) {
      org.apache.hadoop.fs.Path hadoopPath = (org.apache.hadoop.fs.Path) path;
      readFromHdfs(hadoopPath);
      System.gc();
    } else {
      throw new RuntimeException("Invalid path: " + path);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Done in " + (System.currentTimeMillis() - start));
      LOG.info("Number vertices: " + numVertices);
      LOG.info("Number edges: " + numEdges);
    }
  }

  protected void readFromHdfs(org.apache.hadoop.fs.Path hdfsPath) throws IOException {
    FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
    InputStream is = fs.open(hdfsPath);
    readFromInputStream(is);
    is.close();
  }

  protected void readFromFile(Path filePath) throws IOException {
    InputStream is = Files.newInputStream(filePath);
    readFromInputStream(is);
    is.close();
  }

  protected void readFromInputStream(InputStream is) throws IOException {
    if (isBinary && isMultiGraph) {
      readFromInputStreamBinary(is);
    } else {
      readFromInputStreamText(is);
    }
  }

  void write(ObjectOutput out) throws IOException {
      out.writeLong(numVertices);
      out.writeLong(numEdges);
      out.writeBoolean(isEdgeLabelled);
      out.writeBoolean(isFloatLabel);
      out.writeBoolean(isMultiGraph);
      out.writeBoolean(isBinary);
      out.writeObject(name);
  }

  void read(ObjectInput in) throws IOException, ClassNotFoundException{
      numVertices = in.readLong();
      numEdges = in.readLong();
      isEdgeLabelled = in.readBoolean();
      isFloatLabel = in.readBoolean();
      isMultiGraph = in.readBoolean();
      isBinary = in.readBoolean();
      name = (String) in.readObject();
  }

  @Override
  public int getNumberVertices() {
    return (int)numVertices;
  }

  @Override
  public int getNumberEdges() {
    return (int)numEdges;
  }
  @Override
  public boolean isMultiGraph() {
    return isMultiGraph;
  }
  @Override
  public boolean isEdgeLabelled() {
    return isEdgeLabelled;
  }

  public String getName() {
    return name;
  }

  protected abstract void readFromInputStreamText(InputStream is) throws IOException;
  protected abstract void readFromInputStreamBinary(InputStream is) throws IOException;

}
