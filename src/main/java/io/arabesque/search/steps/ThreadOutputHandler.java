package io.arabesque.search.steps;

import io.arabesque.conf.Configuration;
import io.arabesque.utils.ThreadOutput;
import io.arabesque.utils.ThreadOutputBinary;
import io.arabesque.utils.ThreadOutputText;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ThreadOutputHandler {
    private static final Logger LOG = Logger.getLogger(TreeBuilding.class);

    public static ThreadOutput createThreadOutput(int partitionID, boolean append) {
        Configuration arabesque_conf = Configuration.get();
        String OUTPUT_PATH  = arabesque_conf .getString(arabesque_conf .SEARCH_OUTPUT_PATH, arabesque_conf .SEARCH_OUTPUT_PATH_DEFAULT);
        boolean writeInBinary = arabesque_conf .getBoolean(arabesque_conf .SEARCH_WRITE_IN_BINARY, arabesque_conf .SEARCH_WRITE_IN_BINARY_DEFAULT);
        int bufferSize = arabesque_conf .getInteger(arabesque_conf .SEARCH_BUFFER_SIZE, arabesque_conf .SEARCH_BUFFER_SIZE_DEFAULT);
        boolean output_is_active = arabesque_conf .getBoolean(arabesque_conf .SEARCH_OUTPUT_PATH_ACTIVE, arabesque_conf .SEARCH_OUTPUT_PATH_ACTIVE_DEFAULT);


        if(!output_is_active)
            return null;

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.error("Could not get the file system");
            e.printStackTrace();
            System.exit(1);
        }
        Path path = new Path(OUTPUT_PATH, Integer.toString(partitionID));

        ThreadOutput out;

        if (writeInBinary){
            out = new ThreadOutputBinary();
        }
        else {
            out = new ThreadOutputText();
        }

        try {
            out.init(path, fs, bufferSize, append);
        } catch (IOException e) {
            LOG.error("Could not initialize the Thread Output");
            e.printStackTrace();
            System.exit(1);
        }
        return out;
    }

    public static void closeThreadOutput (ThreadOutput out){
        Configuration arabesque_conf = Configuration.get();
        boolean output_is_active = arabesque_conf .getBoolean(arabesque_conf .SEARCH_OUTPUT_PATH_ACTIVE, arabesque_conf .SEARCH_OUTPUT_PATH_ACTIVE_DEFAULT);

        if(out == null || !output_is_active)
            return;

        try {
            out.close();
        } catch (IOException e) {
            LOG.error("Could not close Thread Output. Exiting.");
            e.printStackTrace();
            throw new RuntimeException("Can't close file");
        }
    }
}
