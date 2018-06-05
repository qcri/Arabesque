package io.arabesque.utils;

import io.arabesque.search.trees.SearchEmbedding;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by siganos on 5/15/16.
 */
public class ThreadOutputText implements ThreadOutput {
    private BufferedWriter out;

    @Override
    public void init(Path name, FileSystem fs, int bufferSize, boolean append) throws IOException {
        OutputStreamWriter outputStream;
        if (!append) {
            outputStream = new OutputStreamWriter(fs.create(name, (short) 1));
        } else {
            outputStream = new OutputStreamWriter(fs.append(name, (short) 1));
        }
        out = new BufferedWriter(outputStream,bufferSize);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void write(SearchEmbedding one) throws IOException{
        out.write(one.toOutputString());
    }
}
