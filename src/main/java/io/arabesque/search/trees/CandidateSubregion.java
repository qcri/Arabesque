package io.arabesque.search.trees;

import com.koloboke.function.IntPredicate;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import net.openhft.koloboke.function.IntPredicate;

/**
 * Created by mserafini on 4/26/16.
 */
public class CandidateSubregion implements Writable {
    private static final Logger LOG = Logger.getLogger(Domain.class);

    IntArrayList domainArray = new IntArrayList();
    private int prunedElements = 0;

    void addChild(int vertexId){
        //DEBUG
//        if(domainArray.contains(vertexId)){
//            throw new RuntimeException("Adding a vertex to candidate subregion twice");
//        }

        domainArray.add(vertexId);
    }

    void addChildren(IntArrayList domainArray){
        this.domainArray = domainArray.clone();
    }

    int get (int position){
        return domainArray.getUnchecked(position);
    }

    int size(){
        return domainArray.size();
    }

    void trimPruned(){
        if(prunedElements > 0) {
            domainArray.removeIf(new IntPredicate() {
                @Override
                public boolean test(int value) {
                    return value == -1;
                }
            });
            prunedElements = 0;
        }
    }

    void prune(int vertexId){
        // DEBUG
//        if(domainArray.get(position) == -1){
//            throw new RuntimeException("Tried to prune vertex twice");
//        }

        boolean replaced = domainArray.replace(vertexId, -1);
        if(replaced){
            prunedElements++;
        }
    }

    void clear(){
        domainArray.clear();
        prunedElements = 0;
    }

    boolean isAllPruned(){
        return prunedElements == domainArray.size();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        domainArray.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
//        domainArray.readFields(dataInput);

        int numElements = dataInput.readInt();

        domainArray.ensureCapacity(numElements);

        for (int i = 0; i < numElements; ++i) {
            int val = dataInput.readInt();
            if (val != -1){
                domainArray.add(val);
            }
        }
    }

    @Override
    public String toString() {
        return "CandidateSubregion{" +
                "domainArray=" + domainArray +
                ", prunedElements=" + prunedElements +
                '}';
    }
}

