package io.arabesque.compression;

import io.arabesque.computation.Computation;
import io.arabesque.embedding.Embedding;
import io.arabesque.odag.domain.StorageReader;
import org.apache.hadoop.io.Writable;
import io.arabesque.report.StorageReport;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

public abstract class SimpleStorageStash<O extends SimpleStorage, S extends SimpleStorageStash>
      implements Writable {

   public abstract void addEmbedding(Embedding embedding);

   public abstract void aggregate(O odag);

   public abstract void aggregateUsingReusable(O ezip);

   public abstract void aggregateStash(S value);
   
   public abstract void finalizeConstruction(ExecutorService pool, int parts);

   public abstract boolean isEmpty();

   public abstract int getNumZips();
    
   public abstract Collection<O> getEzips();
    
   public abstract void clear();

   public interface Reader<O extends Embedding> extends Iterator<O> {
   }
   public static class EfficientReader<O extends Embedding> implements Reader<O> {
      private final int numPartitions;
      private final Computation<Embedding> computation;
      private final int numBlocks;
      private final int maxBlockSize;

      private Iterator<? extends SimpleStorage> stashIterator;
      private StorageReader currentReader;
      private boolean currentPositionConsumed = true;

      // #reporting
      private ArrayList<StorageReport> stashReports = new ArrayList<>();

      public EfficientReader(SimpleStorageStash<?,?> stash, Computation<? extends Embedding> computation, int numPartitions, int numBlocks, int maxBlockSize) {
         this.numPartitions = numPartitions;
         this.computation = (Computation<Embedding>) computation;
         this.numBlocks = numBlocks;
         this.maxBlockSize = maxBlockSize;

         stashIterator = stash.getEzips().iterator();
         currentReader = null;
      }

      @Override
      public boolean hasNext() {
         while (true) {
            if (currentReader == null) {
               if (stashIterator.hasNext()) {
                  currentReader = stashIterator.
                     next().
                     getReader(computation, numPartitions, numBlocks, maxBlockSize);
               }
            }

            // No more zips, for sure nothing else to do
            if (currentReader == null) {
               currentPositionConsumed = true;
               return false;
            }

            // If we consumed the current embedding (called next after a previous hasNext),
            // we need to actually advance to the next one.
            if (currentPositionConsumed && currentReader.hasNext()) {
               currentPositionConsumed = false;
               return true;
            }
            // If we still haven't consumed the current embedding (called hasNext but haven't
            // called next), return the same result as before (which is necessarily true).
            else if (!currentPositionConsumed) {
               return true;
            }
            // If we have consumed the current embedding and the current reader doesn't have
            // more embeddings, we need to advance to the next reader so set currentReader to
            // null and let the while begin again (simulate recursive call without the stack
            // building overhead).
            else {
               UPSDomainStorageReadOnly.Reader reader = (UPSDomainStorageReadOnly.Reader)currentReader;
               // #reporting
               stashReports.add(reader.getStorageReport());

               currentReader.close();
               currentReader = null;
            }
         }
      }

      @Override
      public O next() {
         currentPositionConsumed = true;

         return (O) currentReader.next();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      // #reporting
      //*
      public ArrayList<StorageReport> getStashStorageReports() {
         return stashReports;
      }
      //*/
   }
}
