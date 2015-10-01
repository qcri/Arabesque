package io.arabesque;

import io.arabesque.conf.YamlConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ArabesqueRunner implements Tool {
    /**
     * Class logger
     */
    private static final Logger LOG = Logger.getLogger(ArabesqueRunner.class);
    /**
     * Writable conf
     */
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (null == getConf()) {
            conf = new Configuration();
        }

        GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
        YamlConfiguration yamlConfig = new YamlConfiguration(args);
        yamlConfig.load();
        yamlConfig.populateGiraphConfiguration(giraphConf);

        // set up job for various platforms
        final String arabesqueComputationName = giraphConf.get(io.arabesque.conf.Configuration.CONF_COMPUTATION_CLASS);
        final String jobName = "Arabesque: " + arabesqueComputationName;

        // run the job, collect results
        if (LOG.isDebugEnabled()) {
            LOG.debug("Attempting to run computation: " + arabesqueComputationName);
        }

        GiraphJob job = getJob(giraphConf, jobName);
        boolean verbose = yamlConfig.getBoolean("verbose");
        return job.run(verbose) ? 0 : -1;
    }

    protected GiraphJob getJob(GiraphConfiguration conf, String jobName)
            throws IOException {
        return new GiraphJob(conf, jobName);
    }

    /**
     * Execute ArabesqueRunner.
     *
     * @param args Typically command line arguments.
     * @throws Exception Any exceptions thrown.
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ArabesqueRunner(), args));
    }
}
