package io.arabesque.utils;

import io.arabesque.search.trees.SearchEmbedding;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by siganos on 5/15/16.
 */
public class ThreadOutputBinary implements ThreadOutput {
    private BufferedOutputStream out;

    @Override
    public void init(Path name, FileSystem fs, int bufferSize, boolean append) throws IOException {
        DataOutputStream outputStream;
        if (!append) {
            outputStream = new DataOutputStream(fs.create(name, (short) 1));
        } else {
            outputStream = new DataOutputStream(fs.append(name, (short) 1));
        }
        out = new BufferedOutputStream(outputStream,bufferSize);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void write(SearchEmbedding one) throws IOException {
        one.write_to_stream(out);
    }
}
