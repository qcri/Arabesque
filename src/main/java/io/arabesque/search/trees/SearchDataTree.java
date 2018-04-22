package io.arabesque.search.trees;

import io.arabesque.conf.Configuration;
import io.arabesque.search.steps.QueryGraph;
import io.arabesque.utils.collection.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * Created by mserafini on 4/13/16.
 */
public class SearchDataTree implements Writable, Externalizable {
    private static final Logger LOG = Logger.getLogger(SearchDataTree.class);

    public int rootDataVertexId;
    // NOTE! We do NOT have a domain for the root vertex
    private Domain[] domains;
    private int maxSizeEmbedding;
    private boolean scanMatchingOrder = false;
    private boolean injective = true;
    private final boolean TLP = false;
    private int totalPartitions;

    // this is the candidate subregion that we are adding after currEmbedding
    private CandidateSubregion candidateSubregionAdd;

    private ArrayList<SearchDataTree> splits;

    // ###### MATCHING VARIABLES #########

    // matching order position -> DFS position
    private IntArrayList matchingOrder;
    // DFS position -> matching order position
    private IntArrayList inverseMatchingOrder;

    private SearchEmbedding currEmbedding = new SearchEmbedding();

    // this is the candidate subregion for the last element of currEmbedding
    private CandidateSubregion candidateSubregionScan;
    private int candidateSubregionScanPos;

    private boolean stopExpandingCurrEmbedding = false;
    private ArrayList<ExpansionState> stack;
    private ArrayList<ExpansionState> stackPool;
    private QueryGraph queryTree;

    // ###################################

    public boolean fromMessage = false;

    public SearchDataTree(){ }

    public SearchDataTree(int rootDataVertexId, QueryGraph searchQueryTree, int totalPartitions,
                          boolean injective){
        this.rootDataVertexId = rootDataVertexId;

        maxSizeEmbedding = searchQueryTree.getNumberVertices();
        domains = new Domain[maxSizeEmbedding - 1];
        for (int i = 0; i < (maxSizeEmbedding - 1); i++){
            domains[i] = new Domain();
        }

        this.queryTree = searchQueryTree;
        this.injective = injective;
        matchingOrder = new IntArrayList(queryTree.getNumberVertices());
        inverseMatchingOrder = new IntArrayList(queryTree.getNumberVertices());

        this.totalPartitions = totalPartitions;
        splits = new ArrayList<SearchDataTree>(totalPartitions);
    }

    public Domain.DomainIterator getDomainIterator(int DFSPos){
        return new Domain.DomainIterator(domains[DFSPos - 1]);
    }

    public boolean isExtended (int fatherVertexId, int childrenDFSPos){
        return domains[childrenDFSPos-1].hasCandidateSubregion(fatherVertexId);
    }

    public void addCandidateSubregion(int fatherVertexId, int childrenDFSPos, IntArrayList childrenVertexIds){
        domains[childrenDFSPos-1].addChildren(fatherVertexId,childrenVertexIds);
    }

    void addEmbedding(SearchEmbedding embedding){

        // NOTE! precondition is that no two equal embeddings are added this way
        for (int childDFSPos = 1; childDFSPos < embedding.getSize(); childDFSPos++){

            int fatherDFSPos = queryTree.getChildToFatherDFS(childDFSPos);

            // NOTE! We do NOT have a domain for the root vertex
            domains[childDFSPos - 1].addChild(embedding.getAtPos(fatherDFSPos),
                                              embedding.getAtPos(childDFSPos));
        }
    }

    public boolean addExtensionLastEmbedding(SearchEmbedding embedding, int extension){

        if (embedding.getNumWords() == maxSizeEmbedding){
            throw new RuntimeException("QFrag: Hairy!! Trying to addChild extensions to an embedding of maximum size");
        }

        if(embedding != currEmbedding){
            // must only addChild embeddings to the embedding returned by getNextEmbedding()
            throw new RuntimeException("Tried to addChild extension to a different embedding");
        }

        // this logic is to avoid duplicate vertices inside candidate subregions
        // once I have an embedding that expands a father vertex to a child candidate subregion,
        // no other embedding can expand the same father vertex to the same child candidate subregion
        int childDFSPos = currEmbedding.getNumWords();
        int fatherDFSPos = queryTree.getChildToFatherDFS(childDFSPos);
        int fatherVertexId = currEmbedding.getAtPos(fatherDFSPos);

//            LOG.info("QFrag: Adding extension. Father vertex is " + fatherVertexId
//                    + " and child DFS position is " + childDFSPos);

        if(candidateSubregionAdd == null){

            CandidateSubregion newCandidateSubregion =
                    domains[childDFSPos - 1].createAndGetCandidateSubregion(fatherVertexId);
            if (newCandidateSubregion == null){
                return false;
            }

            candidateSubregionAdd = newCandidateSubregion;

//                LOG.info("QFrag: Created new candidate subregion for domain " + (childDFSPos - 1));
        }
        candidateSubregionAdd.addChild(extension);
//            LOG.info("QFrag: Candidate subregion now is " + candidateSubregionAdd);
        domains[childDFSPos-1].incrementCardinality();
        //System.out.println(domains[childDFSPos-1]);
        return true;
    }

    public void prepareScanMatchingOrder(QueryGraph searchQueryTree){
        matchingOrder = searchQueryTree.getMatchingOrder(this, matchingOrder);

//        LOG.info("QFrag: matching order is " + matchingOrder);

        inverseMatchingOrder.clear();
        for (int i = 0; i < matchingOrder.size(); i++){
            // addChild enough elements to do set() later
            inverseMatchingOrder.add(0);
        }
        for (int i = 0; i < matchingOrder.size(); i++){
            inverseMatchingOrder.set(matchingOrder.getUnchecked(i), i);
        }

//        LOG.info("QFrag: inverse matching order is " + inverseMatchingOrder);

        scanMatchingOrder = true;
        currEmbedding.reset();
    }


    private boolean expand(){

        if(currEmbedding.getNumWords() == maxSizeEmbedding){
            throw new RuntimeException("QFrag: Hairy: trying to expand embedding with maximal size: " + currEmbedding);
        }

        // save current domain and position
        CandidateSubregion prevDomainArrayLastWord = candidateSubregionScan;
        int prevDomainPosLastWord = candidateSubregionScanPos;

//         LOG.info("QFrag: getting the next domain for the expansion of embedding " + currEmbedding);

        // move to the next domain that expands the currEmbedding
        candidateSubregionScan =  getNextDomainArray(currEmbedding);
        candidateSubregionScanPos = 0;

        if(candidateSubregionScan != null) {

//                LOG.info("QFrag: next domain is " + candidateSubregionScan + " and pos is " + candidateSubregionScanPos);

            int nextWord = getNextWordInDomain();

//            LOG.info("QFrag: next word is " + nextWord);

            if (nextWord != -1) {

                if (currEmbedding.getNumWords() > 1) {

                    // push the expansion state before the expansion
                    pushStack(prevDomainArrayLastWord,prevDomainPosLastWord);
                }

                currEmbedding.addWord(nextWord);
                return true;
            }
        }

//            LOG.info("QFrag: resetting for embedding " + currEmbedding);

        // currEmbedding cannot be expanded. prune it and reset the domains
        // I get here only if candidateSubregionScan == null or nextWord == -1
        candidateSubregionScan = prevDomainArrayLastWord;
        candidateSubregionScanPos = prevDomainPosLastWord;

        return false;
    }


    private int getNextWordInDomain (){

        int nextWord;

        while (candidateSubregionScanPos < candidateSubregionScan.size()) {

            nextWord = candidateSubregionScan.get(candidateSubregionScanPos++);

//            if(currEmbedding.getAtPos(0) == 1247 && currEmbedding.getAtPos(1) == 8543 && nextWord == 8543) {
//                LOG.info("QFrag: next word is going to be " + nextWord + ", currembedding is "
//                        + currEmbedding + "  and " + currEmbedding.contains(nextWord) + " and " + injective);
//            }

            // skip pruned elements
            if(nextWord != -1) {
                // TODO must be optimized
                if (!(injective && currEmbedding.contains(nextWord))) {
                    return nextWord;
                }
            }
        }
        return -1;
    }

    private SearchEmbedding getSibling(){

        if(candidateSubregionScan == null){
            return null;
        }

        while (true) {

            // look for the sibling of currEmbedding
            // if none is available, pop expansion state until empty

            int nextWord = -1;
            // skip considering all unnecessary combinations during expansion
            //TODO this can be probably generalized beyond leaf vertices
            if (scanMatchingOrder || !queryTree.isLeaf(currEmbedding.getSize()-1)){
//                LOG.info("QFrag: not skipping sibling");
                nextWord = getNextWordInDomain();
            }

//            if(currEmbedding.getAtPos(0) == 1247 && currEmbedding.getAtPos(1) == 8543) {
//                LOG.info("QFrag: next word is " + nextWord);
//            }

            if (nextWord == -1) {
                // no sibling available

                if (stack.isEmpty()) {
                    // nothing more to return from this tree
                    return null;
                }

                // pop and restore expansion state
                popStack();

//                LOG.info("QFrag: popped embedding " + currEmbedding);

            } else {

                currEmbedding.removeLastWord();
                currEmbedding.addWord(nextWord);

                return currEmbedding;
            }
        }
    }

    public SearchEmbedding getNextEmbedding() {

        candidateSubregionAdd = null;

        if (currEmbedding.isEmpty()) {
//            LOG.info("QFrag: currEmbedding is empty");

            if (stack == null) {
                stack = new ArrayList<>();

                stackPool = new ArrayList<>();
                for (int i = 0; i <= maxSizeEmbedding; i++) {
                    stackPool.add(new ExpansionState());
                }
            } else {
                while(!stack.isEmpty()){
                    ExpansionState state = stack.remove(stack.size()-1);
                    stackPool.add(state);
                }
            }

            currEmbedding.addWord(rootDataVertexId);

            stopExpandingCurrEmbedding = false;

            return currEmbedding;

        } else {

//            if(currEmbedding.getAtPos(1) == 3306) {
//                LOG.info("QFrag: getting next embedding from " + currEmbedding);
//            }
            //System.out.println(stopExpandingCurrEmbedding+" "+maxSizeEmbedding);
            if (!stopExpandingCurrEmbedding
                    && currEmbedding.getNumWords() < maxSizeEmbedding) {

//                if(currEmbedding.getAtPos(1) == 3306) {
//                    LOG.info("QFrag: expanding");
//                }
                boolean isExpanded = expand();

                if (!isExpanded) {
//                    if(currEmbedding.getAtPos(1) == 3306) {
//                        LOG.info("QFrag: not expanded");
//                    }
                    //System.out.println("1");
                    return getSibling();
                }
                else {
//                    System.out.println("QFrag: expanded");
                    return currEmbedding;
                }
            }
            else{
                // if I have pruned the current embedding or if the current embedding is of maximum size
                // return sibling (if available)

//                if(currEmbedding.getAtPos(1) == 3306) {
//                    if (stopExpandingCurrEmbedding) {
//                        LOG.info("QFrag: pruned last embedding: getting sibling of " + currEmbedding);
//                    } else {
//                        LOG.info("QFrag: max size: getting sibling of " + currEmbedding);
//                    }
//                }

                stopExpandingCurrEmbedding = false;
                //System.out.println("3");
                return getSibling();
            }
        }
    }

    private CandidateSubregion getNextDomainArray(SearchEmbedding embedding){

        int childDFSPos;
        if (scanMatchingOrder) {
            int childMatchingOrderPos = embedding.getNumWords();
            childDFSPos = matchingOrder.getUnchecked(childMatchingOrderPos);
        }
        else{
            childDFSPos = embedding.getNumWords();
        }

        int fatherVertexId;
        int fatherDFSPos = queryTree.getChildToFatherDFS(childDFSPos);
        if(scanMatchingOrder) {
            int fatherEmbeddingPos = inverseMatchingOrder.getUnchecked(fatherDFSPos);
            fatherVertexId = embedding.getAtPos(fatherEmbeddingPos);
        }
        else{
            fatherVertexId = embedding.getAtPos(fatherDFSPos);
        }

//        LOG.info("QFrag: Extending from father " + fatherVertexId + " to DFS position " + childDFSPos);

        Domain currDomain = domains[childDFSPos - 1];
        return currDomain.getCandidateSubregion(fatherVertexId);
    }

    public void pruneLastEmbeddingMatching(){
        // should not prune subregions here because it is the embedding as a whole that does not match,
        // not some particular vertex that is incorrect
        stopExpandingCurrEmbedding = true;
    }

    public void pruneExploration(SearchEmbedding embedding, int DFSPos){

        stopExpandingCurrEmbedding = true;

        if(DFSPos < 1){
            return;
        }

        while (stack.size() > DFSPos - 1){
            popStack();
        }

        if(TLP){
            return;
        }

        int fatherDFSPos = queryTree.getChildToFatherDFS(DFSPos);
        int fatherVertex = embedding.getAtPos(fatherDFSPos);
        int childVertex = embedding.getAtPos(DFSPos);

        CandidateSubregion childSubregion = domains[DFSPos - 1].getCandidateSubregion(fatherVertex);
        if(childSubregion == null){
            throw new RuntimeException("QFrag: Hairy: trying to prune from an empty candidate subregion");
        }
//        LOG.info("QFrag: pruning vertex " + childVertex + " at DFS position " + DFSPos
//                + " with father " + fatherVertex + " at DFS position " + fatherDFSPos
//                + " from subregion " + childSubregion);
        childSubregion.prune(childVertex);
//        LOG.info("QFrag: subregion now is " + childSubregion);

        if(childSubregion.isAllPruned()){
            pruneExploration(embedding, fatherDFSPos);
        }
    }

    public IntArrayList getMatchingOrder(){
        return matchingOrder;
    }

    public IntArrayList getInverseMatchingOrder(){
        return inverseMatchingOrder;
    }

    public int getCardinality(int DFSPosition){
        if(DFSPosition == 0){
            return 1;
        }
        return domains[DFSPosition - 1].getCardinality();
    }

    private void pushStack(CandidateSubregion subregion, int position){
        ExpansionState pooledState = stackPool.remove(stackPool.size()-1);

        pooledState.embedding.setFrom(currEmbedding);
        pooledState.candidateSubregionLastWord = subregion;
        pooledState.candidateSubregionLastWordPos = position;

//                        LOG.info("QFrag: pushing embedding " + currEmbedding
//                                + " and subregion " + prevDomainArrayLastWord
//                                + " with reference " + System.identityHashCode(prevDomainArrayLastWord));

        stack.add(pooledState);

        if (stack.size() > maxSizeEmbedding) {
            throw new RuntimeException("QFrag: Hairy: stack size is too large");
        }
    }

    private void popStack(){
        ExpansionState fromStack = stack.remove(stack.size()-1);
        stackPool.add(fromStack);

        currEmbedding.setFrom(fromStack.embedding);
        candidateSubregionScan = fromStack.candidateSubregionLastWord;
        candidateSubregionScanPos = fromStack.candidateSubregionLastWordPos;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFields(in);
    }

    private class ExpansionState {
        private SearchEmbedding embedding;
        private CandidateSubregion candidateSubregionLastWord;
        private int candidateSubregionLastWordPos;

        private ExpansionState(){
            embedding = new SearchEmbedding();
            candidateSubregionLastWord = new CandidateSubregion();
            candidateSubregionLastWordPos = -1;
        }

        private ExpansionState(SearchEmbedding embedding, CandidateSubregion candidateSubregionLastWord, int candidateSubregionLastWordPos) {
            this.embedding = embedding;
            this.candidateSubregionLastWord = candidateSubregionLastWord;
            this.candidateSubregionLastWordPos = candidateSubregionLastWordPos;
        }
    }

    public boolean isRootPruned(){
        // recursive pruning only goes up to the child domain of the root. we check that to see if the root itself is pruned
        if(domains[0].getCandidateSubregion(rootDataVertexId) == null){
            return false;
        }
        return domains[0].getCandidateSubregion(rootDataVertexId).isAllPruned();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if(isRootPruned()){
            //LOG.info("Skipped writing a fully pruned SearchDataTree for root " + rootDataVertexId);
            return;
        }

        dataOutput.writeInt(rootDataVertexId);
        dataOutput.writeInt(domains.length);
        for(Domain domain : domains){
            domain.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        rootDataVertexId = dataInput.readInt();
        int numDomains = dataInput.readInt();

        domains = new Domain[numDomains];
        for (int i = 0; i < numDomains; i++) {
            domains[i] = new Domain();
            domains[i].readFields(dataInput);
        }

        Configuration conf = Configuration.get();
        injective = conf.getBoolean("QUERY_INJECTIVE", true);
    }

    public void setSearchQueryTree(QueryGraph searchQueryTree){
        queryTree = searchQueryTree;
        maxSizeEmbedding = searchQueryTree.getNumberVertices();

        matchingOrder = new IntArrayList(queryTree.getNumberVertices());
        inverseMatchingOrder = new IntArrayList(queryTree.getNumberVertices());
    }

//    public int getCardinalitySplitDomain(){
//        int splitDomainPos = queryTree.getDFSPosSplitDomain(this) - 1;
//        return domains[splitDomainPos].getCardinality();
//    }

    public long getMatchingCost(){

        long costPerEmbedding = queryTree.getMatchingCostPerEmbedding(this);
        return costPerEmbedding * size();

//        long numEmbeddingsMatched = 0;
//        if (numEmbeddingsExplored == 0) {
//            // approximate the number of full embeddings
//            for (int i = 0; i < domains.length; i++) {
//                numEmbeddingsMatched *= domains[i].getCardinality();
//            }
//        } else {
//            numEmbeddingsMatched = queryTree.getNumberEmbeddingsMatched(this, numEmbeddingsExplored);
//        }
//        return costPerEmbedding * numEmbeddingsMatched;

//        int splitDomainPos = queryTree.getDFSPosSplitDomain(this) - 1;
//        if(splitDomainPos < 0){
//            return -1;
//        }
//        IntArrayList destArrayDFSPos = queryTree.getCrossEdgesDestVertex(splitDomainPos);
//        long cost = domains[splitDomainPos].getCardinality();
//        for (int i = 0; i < destArrayDFSPos.size(); i++){
//            cost *= domains[destArrayDFSPos.get(i) - 1].getCardinality();
//        }
//        return cost;
    }

    public ArrayList<SearchDataTree> split (){

        int splitDomainPos = queryTree.getDFSPosSplitDomain(this) - 1;
        if(splitDomainPos < 0){
            return null;
        }
        Domain splitDomain = domains[splitDomainPos];

//        LOG.info("Split domain position is " + splitDomain);

        if(splitDomain.getCardinality() < totalPartitions){
            return null;
        }

        IntOpenHashSet splitDomainsPosSet = new IntOpenHashSet();
        splitDomainsPosSet.add(splitDomainPos);

        Random random = new Random(Thread.currentThread().getId() + 10);
        int destSplitIndex = random.nextInt(totalPartitions);

        if(splits.isEmpty()) {
            for (int i = 0; i < totalPartitions; i++) {
                splits.add(new SearchDataTree(rootDataVertexId, queryTree, totalPartitions, injective));
            }
        } else {
            for(int i = 0; i < totalPartitions; i++){
                splits.get(i).resetTo(rootDataVertexId);
            }
        }


        Iterator<Map.Entry<Integer, CandidateSubregion>> iterator = splitDomain.iterator();
        while (iterator.hasNext()) {

            Map.Entry<Integer, CandidateSubregion> entry = iterator.next();

            int fatherVertexId = entry.getKey();
            CandidateSubregion candidateSubregion = entry.getValue();

            for (int i = 0; i < candidateSubregion.size(); i++) {

                int splitVertex = candidateSubregion.get(i);

                if(splitVertex == -1){
                    continue;
                }

                SearchDataTree destTree = splits.get(destSplitIndex % totalPartitions);
                destSplitIndex ++;
                //            LOG.info("QFrag: adding vertex " + splitVertex + " to domain "
                //                    + currChildDomainPos + " for split number " + i % results.size());
                destTree.domains[splitDomainPos].addChild(fatherVertexId, splitVertex);
                //            LOG.info("QFrag: result domain " + destTree.domains[currChildDomainPos]);

                // recursively split the children domains of the splitVertex
                splitDomain(splitVertex, splitDomainPos, destTree, splitDomainsPosSet);
                //            LOG.info("QFrag: result domain again " + destTree.domains[currChildDomainPos]);
            }
        }

        // copy all other domains that were not split
        for (int domainPos = 0; domainPos < queryTree.getNumberVertices() - 1; domainPos++){
            if(!splitDomainsPosSet.contains(domainPos)){
                for(SearchDataTree tree : splits){
//                    LOG.info("QFrag: Copying domain " + domainPos + "\n QFrag: " + this.domains[domainPos].toString());
                    tree.domains[domainPos] = this.domains[domainPos];
                }
            }
        }

        return splits;
    }

    private void splitDomain(int fatherVertexId, int fatherDomainPos, SearchDataTree destTree, IntOpenHashSet splitDomainPos){
        // TODO could split the same domain twice because the same father appears in two candidate regions

        // DFSPos = domainPos + 1 because there is no domain for root
        for(int childDFSPos : queryTree.getFatherToChildrenDFS(fatherDomainPos + 1)){

            int childDomainPos = childDFSPos - 1;
            splitDomainPos.add(childDomainPos);

            CandidateSubregion sourceCandidateSubregion =
                    this.domains[childDomainPos].getCandidateSubregion(fatherVertexId);
            if(sourceCandidateSubregion != null) {
                destTree.domains[childDomainPos].addCandidateSubregion(fatherVertexId, sourceCandidateSubregion);

                if(sourceCandidateSubregion.domainArray != null) {
                    // recursively copy the children of each child vertex to the destination tree
                    for (int childVertex : sourceCandidateSubregion.domainArray) {
                        splitDomain(childVertex, childDomainPos, destTree, splitDomainPos);
                    }
                }
            }
        }
    }

    public boolean hasFullEmbedding(){
        return domains[domains.length-1].getCardinality() > 0;
    }

    public void resetTo (int newRootDataVertexId){
        this.rootDataVertexId = newRootDataVertexId;

        scanMatchingOrder = false;
        candidateSubregionAdd = null;
        if(matchingOrder != null) {
            matchingOrder.clear();
        }
        if (inverseMatchingOrder != null) {
            inverseMatchingOrder.clear();
        }
        candidateSubregionScan = null;
        candidateSubregionScanPos = 0;
        stopExpandingCurrEmbedding = false;
        if (currEmbedding != null){
            currEmbedding.reset();
        }

        for (int i = 0; i < (maxSizeEmbedding - 1); i++){
            domains[i].clear();
        }

        if(stack != null) {
            while (!stack.isEmpty()) {
                ExpansionState state = stack.remove(stack.size()-1);
                stackPool.add(state);
            }
        }
        fromMessage = false;
    }

    @Override
    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append("\nQFrag: root: ").append(rootDataVertexId).append("\n");
        for (int i = 0; i < domains.length; i++){
            str.append("QFrag: domain # ").append(i).append("\n");
            str.append(domains[i].toString());
        }
        return str.toString();
    }

    public int size(){
        int sum = 1; // root
        for (int i = 0; i < domains.length; i++){
            sum += domains[i].getCardinality();
        }
        return sum;
    }

    //########## USED FOR BFS EXPANSION

    //    IntArrayList getNextEmbedding() {
//
//        if(!scanStarted){
//            candidates = new LinkedList<>();
//
//            currEmbedding = new IntArrayList ();
//            currEmbedding.addChild(rootDataVertexId);
//
//            // skip the root
//            matchingOrderPos = 0;
//
//            scanStarted = true;
//
//            return currEmbedding;
//        }
//        else {
//            // the embedding has been returned already. We need to clean up the last word
//            currEmbedding.removeLast();
//        }
//
//        int nextWord = -1;
//        boolean nextWordFound = false;
//
//        while(!nextWordFound) {
//
//            if (candidateSubregionScan == null || candidateSubregionScanPos >= candidateSubregionScan.size()) {
//
//                // there are no more extensions for the current embedding to the current domain
//                // move to the next embedding because we expand one domain at a time
//
//                IntArrayList newEmbedding = candidates.poll();
//
//                if (newEmbedding == null) {
//                    // if there are no more candidates, this tree terminates
//                    return null;
//                }
//
//                if (newEmbedding.size() > currEmbedding.size()) {
//
//                    // I have finished to extend all embeddings to the current domain
//                    // move to next domain: update currDomain and currFatherEmbeddingPos
//
//                    currChildDomainDFS = matchingOrder.get(++matchingOrderPos);
//                    currDomain = domains[currChildDomainDFS - 1];
//
//                    int currFatherDomainDFS = QueryGraph.getChildToFatherDFS(currChildDomainDFS);
//                    currFatherEmbeddingPos = inverseMatchingOrder.get(currFatherDomainDFS);
//                }
//
//                // start expanding the new embedding to currDomain
//                // update currEmbedding and currFatherVertexId
//
//                currEmbedding = newEmbedding;
//
//                int currFatherVertexId = currEmbedding.get(currFatherEmbeddingPos);
//
//                candidateSubregionScan = currDomain.getNextDomainArray(currFatherVertexId);
//                candidateSubregionScanPos = 0;
//
//                // DEBUG
//                if (candidateSubregionScan.size() == 0) {
//                    LOG.info("QFrag: Hairy: embedding " + currEmbedding + " has no edge from father " + currFatherVertexId
//                            + " to the next domain " + currChildDomainDFS + ". Matching order is " + matchingOrder);
//                }
//
//            }
//
//            nextWord = candidateSubregionScan.get(candidateSubregionScanPos++);
//
//            nextWordFound = true;
//
//            if (injective) {
//                for (int oldWord : currEmbedding) {
//                    if (nextWord == oldWord) {
//                        nextWordFound = false;
//                        break;
//                    }
//                }
//            }
//        }
//
//        currEmbedding.addChild(nextWord);
//
//        return currEmbedding;
//    }
//
//    void expandLastEmbedding(){
//        // I need to clone currEmbedding because I will reuse it in the expansion
//        candidates.addChild(currEmbedding.clone());
//    }


}
