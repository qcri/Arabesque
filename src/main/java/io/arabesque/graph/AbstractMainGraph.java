package io.arabesque.graph;

import io.arabesque.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class AbstractMainGraph implements MainGraph {
  protected long numVertices;
  protected long numEdges;

  protected boolean isEdgeLabelled;
  protected boolean isFloatLabel;
  protected boolean isMultiGraph;
  protected boolean isBinary;

  protected String name;

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
  private void init(String name, boolean isEdgeLabelled, boolean isMultiGraph) {
    this.isEdgeLabelled = isEdgeLabelled;
    this.isMultiGraph = isMultiGraph;
    this.name = name;
    reset();
  }

  private void init(String name) {//boolean isEdgeLabelled, boolean isMultiGraph) {
    Configuration conf = Configuration.get();
    isEdgeLabelled = conf.isGraphEdgeLabelled();
    isMultiGraph = conf.isGraphMulti();
    isFloatLabel = conf.isFloatEdge();
    isBinary = conf.isBinaryInputFile();
    this.name = name;
    reset();
  }

  private void init(Object path) throws IOException {
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
