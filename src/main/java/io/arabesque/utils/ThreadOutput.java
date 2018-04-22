package io.arabesque.utils;

import io.arabesque.search.trees.SearchEmbedding;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Based on conf switch between different types of output.
 * Created by siganos on 5/15/16.
 */
public interface ThreadOutput {
    void init(Path name, FileSystem fs, int bufferSize, boolean append) throws IOException;
    void close() throws IOException;
    void write(SearchEmbedding one) throws IOException;
}
