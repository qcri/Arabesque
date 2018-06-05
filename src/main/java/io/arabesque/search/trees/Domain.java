package io.arabesque.search.trees;

import io.arabesque.utils.collection.IntArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by mserafini on 4/13/16.
 */
public class Domain implements Writable {
    private static final Logger LOG = Logger.getLogger(io.arabesque.search.trees.Domain.class);

    // maps father vertex ids to children in this domain
    // the father is in a specific father domain dictated by the DFS order
    private Int2ObjectOpenHashMap<CandidateSubregion> fatherToChildren = new Int2ObjectOpenHashMap<>();
    private int cardinality = 0;
    // the following two variables are to reuse the SearchDataTree when it is cleared
    private ArrayList<CandidateSubregion> candidateSubregionPool = new ArrayList<>(50);
    private int nextFreeCS = 0;

    public Domain(){
        for(int i = 0; i < 50; i++){
            candidateSubregionPool.add(new CandidateSubregion());
        }
    }

    void addChild(int fatherVertexId, int childVertexId){

        CandidateSubregion candidateSubregion = fatherToChildren.get(fatherVertexId);
        if (candidateSubregion == null){
            if (nextFreeCS < candidateSubregionPool.size()){
                candidateSubregion = candidateSubregionPool.get(nextFreeCS);
                candidateSubregion.clear();
                nextFreeCS++;
            } else {
                candidateSubregion = new CandidateSubregion();
                candidateSubregionPool.add(candidateSubregion);
                nextFreeCS++;
            }
            fatherToChildren.put(fatherVertexId, candidateSubregion);
        }

//        if(!candidateSubregion.contains(childVertexId)){
            candidateSubregion.addChild(childVertexId);
            cardinality++;
//        }
    }

    void addChildren (int fatherVertexId, IntArrayList childrenVertexIds){
        CandidateSubregion candidateSubregion = fatherToChildren.get(fatherVertexId);
        if (candidateSubregion == null){
            if (nextFreeCS < candidateSubregionPool.size()){
                candidateSubregion = candidateSubregionPool.get(nextFreeCS);
                candidateSubregion.clear();
                nextFreeCS++;
            } else {
                candidateSubregion = new CandidateSubregion();
                candidateSubregionPool.add(candidateSubregion);
                nextFreeCS++;
            }
            fatherToChildren.put(fatherVertexId, candidateSubregion);
        }
        candidateSubregion.addChildren(childrenVertexIds);
        cardinality += childrenVertexIds.size();
    }

    void addCandidateSubregion(int fatherVertexId, CandidateSubregion subregion){
        if(fatherToChildren.get(fatherVertexId) == null) {
            fatherToChildren.put(fatherVertexId, subregion);
            cardinality += subregion.size();
        }
    }

    void incrementCardinality(){
        cardinality++;
    }

    int getCardinality(){
        return cardinality;
    }

    CandidateSubregion getCandidateSubregion(int fatherVertexId){
        return fatherToChildren.get(fatherVertexId);
    }

    CandidateSubregion createAndGetCandidateSubregion(int fatherVertexId){
        CandidateSubregion candidateSubregion = fatherToChildren.get(fatherVertexId);

        if (candidateSubregion == null){
            if (nextFreeCS < candidateSubregionPool.size()){
                candidateSubregion = candidateSubregionPool.get(nextFreeCS);
                candidateSubregion.clear();
                nextFreeCS++;
            } else {
                candidateSubregion = new CandidateSubregion();
                candidateSubregionPool.add(candidateSubregion);
                nextFreeCS++;
            }
            fatherToChildren.put(fatherVertexId, candidateSubregion);
            return  candidateSubregion;
        }

        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(fatherToChildren.size());

        for(Int2ObjectMap.Entry<CandidateSubregion> fatherToChildrenEntry : fatherToChildren.int2ObjectEntrySet()){

            int fatherVertexId = fatherToChildrenEntry.getKey();
            CandidateSubregion children = fatherToChildrenEntry.getValue();

            if(children != null && !children.isAllPruned()){
                dataOutput.writeInt(fatherVertexId);
                children.write(dataOutput);
            } else {
                dataOutput.writeInt(-1);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int numCandidateSubregions = dataInput.readInt();
        int newCardinality = 0;
        for (int i = 0; i < numCandidateSubregions; i++){
            int fatherVertexId = dataInput.readInt();
            if(fatherVertexId != -1) {
                CandidateSubregion children = new CandidateSubregion();
                children.readFields(dataInput);
                newCardinality += children.size();
                fatherToChildren.put(fatherVertexId, children);
            }
        }
        this.cardinality = newCardinality;
    }

    public void clear(){
        nextFreeCS = 0;
        fatherToChildren.clear();
        cardinality = 0;
    }

    public Iterator<Map.Entry<Integer,CandidateSubregion>> iterator(){
        return fatherToChildren.entrySet().iterator();
    }

    public boolean hasCandidateSubregion (int fatherVertexId){
        return (fatherToChildren.get(fatherVertexId) != null);
    }

    @Override
    public String toString(){
        StringBuffer str = new StringBuffer();
        for (Int2ObjectMap.Entry<CandidateSubregion> entry : fatherToChildren.int2ObjectEntrySet()){
            str.append("QFrag: Candidate subregion for father " + entry.getKey() + "\n");
            str.append("QFrag: " + entry.getValue().toString() + "\n");
        }
        return str.toString();
    }

    public static class DomainIterator {
        Iterator<CandidateSubregion> CSIterator;
        CandidateSubregion currCS;
        int CSPos;

        public DomainIterator(io.arabesque.search.trees.Domain d){
            CSIterator = d.fatherToChildren.values().iterator();
            currCS = null;
            CSPos = 0;
        }

        public int nextInt() {
            if (currCS == null){
                currCS = CSIterator.next();
            }
            if (CSPos >= currCS.size()){
                CSPos = 0;
                do {
                    currCS = CSIterator.next();
                } while (currCS.size() == 0);
            }
            return currCS.get(CSPos++);
        }

        public boolean hasNext() {
            if (currCS == null){
                if (!CSIterator.hasNext()) {
                    return false;
                }
                currCS = CSIterator.next();
            }
            if (CSPos < currCS.size()){
                return true;
            } else {
                CSPos = 0;
                do {
                    if(!CSIterator.hasNext()){
                        return false;
                    }
                    currCS = CSIterator.next();
                } while (currCS.size() == 0);
            }
            return true;
        }
    }

}