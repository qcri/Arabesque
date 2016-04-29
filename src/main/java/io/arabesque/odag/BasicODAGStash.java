package io.arabesque.odag;

import io.arabesque.computation.Computation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.StorageReader;
import io.arabesque.odag.domain.StorageStats;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;

public abstract class BasicODAGStash<O extends BasicODAG, S extends BasicODAGStash>
      implements Writable, Externalizable {
    
   public abstract void addEmbedding(Embedding embedding);

   public abstract void aggregate(O odag);

   public abstract void aggregateUsingReusable(O ezip);

   public abstract void aggregate(S value);
   
   public abstract void finalizeConstruction(ExecutorService pool, int parts);

   public abstract boolean isEmpty();

   public abstract int getNumZips();

   public abstract void clear();

   public interface Reader<O extends Embedding> extends Iterator<O> {
   }
}
