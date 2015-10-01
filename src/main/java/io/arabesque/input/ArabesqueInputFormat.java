package io.arabesque.input;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class ArabesqueInputFormat extends GeneratedVertexInputFormat<IntWritable, NullWritable, NullWritable> {

    @Override
    public VertexReader<IntWritable, NullWritable, NullWritable>
    createVertexReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new ArabesqueVertexReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint)
            throws IOException, InterruptedException {
        // Some hardcoding based on Giraph behaviour
        int numWorkers = minSplitCountHint / GiraphConfiguration.NUM_INPUT_THREADS.get(getConf());
        minSplitCountHint = numWorkers * numWorkers;

        int userPartitionCount = GiraphConfiguration.USER_PARTITION_COUNT.get(getConf());

        if (userPartitionCount != -1) {
            minSplitCountHint = userPartitionCount;
        }

        if (minSplitCountHint % numWorkers != 0) {
            throw new RuntimeException("Unbalanced partition count: " + minSplitCountHint +
                    " partitions for " + numWorkers +
                    " workers (" + (((double) minSplitCountHint) / numWorkers) +
                    " partitions per worker)");
        }

        // This is meaningless, the VertexReader will generate all the test
        // data.
        List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
        for (int i = 0; i < minSplitCountHint; ++i) {
            inputSplitList.add(new BspInputSplit(i, minSplitCountHint));
        }
        return inputSplitList;
    }

    public class ArabesqueVertexReader extends VertexReader<IntWritable, NullWritable, NullWritable> {
        private BspInputSplit inputSplit;
        private boolean generated;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            this.inputSplit = (BspInputSplit) inputSplit;
            generated = false;
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return !generated;
        }

        @Override
        public Vertex<IntWritable, NullWritable, NullWritable> getCurrentVertex() throws IOException, InterruptedException {
            Vertex<IntWritable, NullWritable, NullWritable> vertex =
                    getConf().createVertex();
            IntWritable id = new IntWritable(inputSplit.getSplitIndex());

            List<Edge<IntWritable, NullWritable>> edges = Collections.emptyList();
            vertex.initialize(id, NullWritable.get(), edges);

            generated = true;
            return vertex;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return generated ? 100 : 0;
        }
    }
}
