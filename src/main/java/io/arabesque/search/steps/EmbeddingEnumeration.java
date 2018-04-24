package io.arabesque.search.steps;

import io.arabesque.QfragRunner;
import io.arabesque.conf.SparkConfiguration;
import io.arabesque.graph.UnsafeCSRGraphSearch;
import io.arabesque.search.trees.SearchDataTree;
import io.arabesque.utils.ThreadOutput;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class EmbeddingEnumeration
        implements Function2<Integer, Iterator<Tuple2<Integer, Iterable<SearchDataTree>>>, Iterator<Integer>> {
    private static final Logger LOG = Logger.getLogger(EmbeddingEnumeration.class);

    private Broadcast<SparkConfiguration> configBC;
    private Broadcast<QueryGraph> queryGraphBC;

    // accumulators
    private Map<String, CollectionAccumulator<Long>> accums;
    private long init_Start_Time = 0;
    private long init_Finish_Time = 0;
    private long computation_Start_Time = 0;
    private long computation_Finish_Time = 0;

    public EmbeddingEnumeration(Broadcast<SparkConfiguration> configBC, Broadcast<QueryGraph> queryGraphBC, Map<String, CollectionAccumulator<Long>> _accums){
        super();

        init_Start_Time = System.currentTimeMillis();

        this.configBC = configBC;
        this.queryGraphBC = queryGraphBC;

        this.accums = _accums;

        String log_level = this.configBC.value().getLogLevel();
        LOG.fatal("Setting log level to " + log_level);
        LOG.setLevel(Level.toLevel(log_level));
/*        LogManager.getRootLogger().setLevel(Level.FATAL);
        Logger.getLogger("org").setLevel(Level.FATAL);
        Logger.getLogger("akka").setLevel(Level.FATAL);
        Logger.getLogger("spark").setLevel(Level.FATAL);
        Logger.getLogger("executor").setLevel(Level.FATAL);
        Logger.getLogger("memory").setLevel(Level.FATAL);
        Logger.getLogger("steps").setLevel(Level.FATAL);
        Logger.getLogger("storage").setLevel(Level.FATAL);
        Logger.getLogger("util").setLevel(Level.FATAL);*/
    }

    @Override
    public Iterator<Integer> call(Integer partitionId, Iterator<Tuple2<Integer, Iterable<SearchDataTree>>> v2) throws Exception {
        if (v2 != null && v2.hasNext()){

            configBC.value().initialize();

            Tuple2<Integer, Iterable<SearchDataTree>> iter = v2.next();
            Iterator<SearchDataTree> msgs = iter._2().iterator();

            // Modified from QFrag
            // UnsafeCSRGraphSearch dataGraph = io.arabesque.conf.Configuration.get().getMainGraph();
            //UnsafeCSRGraphSearch dataGraph = (UnsafeCSRGraphSearch)(io.arabesque.conf.Configuration.get().getMainGraph());
            UnsafeCSRGraphSearch dataGraph = io.arabesque.conf.Configuration.get().getSearchMainGraph();
            QueryGraph queryGraph = queryGraphBC.getValue();

            init_Finish_Time = System.currentTimeMillis();
            computation_Start_Time = System.currentTimeMillis();

            ThreadOutput out = ThreadOutputHandler.createThreadOutput(partitionId, true);
            CrossEdgeMatchingLogic crossEdgeMatchingLogic = new CrossEdgeMatchingLogic(dataGraph, queryGraph, out);

            while(msgs.hasNext()) {
                SearchDataTree split = msgs.next();
                split.setSearchQueryTree(queryGraph);
                crossEdgeMatchingLogic.matchCrossEdges(split);
            }

            ThreadOutputHandler.closeThreadOutput(out);
        }

        // TODO we actually don't need to output an RDD. All matches are stored to HDFS.
        ArrayList<Integer> list = new ArrayList();
        list.add(0);

        computation_Finish_Time = System.currentTimeMillis();
        flushAccumulators();

        return list.iterator();
    }

    private void accumulate(Long value, CollectionAccumulator<Long> _accum) {
        _accum.add( value );
    }

    private void flushAccumulators() {
        // Now calculate the computation runtime
        accumulate(init_Start_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_INIT_START_TIME));
        accumulate(init_Finish_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_INIT_FINISH_TIME));
        accumulate(computation_Start_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_COMPUTATION_START_TIME));
        accumulate(computation_Finish_Time, accums.get(QfragRunner.EMBEDDING_ENUMERATION_COMPUTATION_FINISH_TIME));
    }
}
