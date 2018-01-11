package io.arabesque.report;

import java.math.BigInteger;

/**
 * Created by ehussein on 10/1/17.
 */
public class StorageReport {
    // num of actual embeddings stored in the storage
    public long numActualEmbeddings = 0;
    // Total number of enumerations the storage could hold
    public long numEnumerations = 0;
    public BigInteger biNumEnmEnumerations = BigInteger.ZERO;
    // used to show load balancing between partitions
    public long numCompleteEnumerationsVisited = 0;
    // how many invalid embeddings this storage/partition generated
    public long numSpuriousEmbeddings = 0L;
    public int storageId = 0;

    public int[] domainSize = null;
    public int[] explored = null;
    public int[] pruned = null;

    public void initReport(int numberOfDomains) {
        pruned = new int[numberOfDomains];
        explored = new int[numberOfDomains];
        domainSize = new int[numberOfDomains];
        initArray(pruned, 0);
        initArray(explored, 0);
        initArray(domainSize, 0);
    }

    private void initArray(int[] arr, int value) {
        for(int i = 0; i < arr.length; ++i)
            arr[i] = value;
    }

    @Override
    public String toString() { return toJSONString(); }

    public String toNormalString() {
        StringBuilder str = new StringBuilder();

        str.append("Storage" + storageId + "_Report: {");
        str.append("\nNumEnumerations=" + numEnumerations);
        str.append("\nNumStoredEmbeddings=" + numActualEmbeddings);
        str.append("\nNumCompleteEnumerationsVisited=" + numCompleteEnumerationsVisited);
        str.append("\nNumSpuriousEmbeddings=" + numSpuriousEmbeddings);
        for(int i = 0; i < domainSize.length; ++i) {
            str.append("\nDomain" + i + "Size=" + domainSize[i] + ", ");
            str.append("ExploredInDomain" + i + "=" + explored[i] + ", ");
            str.append("PrunedInDomain" + i + "=" + pruned[i]);
        }
        str.append("\n}");

        return str.toString();
    }

    public String toJSONString() {
        StringBuilder str = new StringBuilder();
        StringBuilder domainSizeStr = new StringBuilder();
        StringBuilder prunedStr = new StringBuilder();
        StringBuilder exploredStr = new StringBuilder();

        str.append("{");
        str.append("\"StrgID\":" + storageId + ", ");
        str.append("\"Embds\":" + numEnumerations + ", ");
        str.append("\"ActEmbds\":" + numActualEmbeddings + ", ");
        str.append("\"CmpltEmbds\":" + numCompleteEnumerationsVisited + ", ");
        str.append("\"SprsEmbds\":" + numSpuriousEmbeddings + ", ");

        domainSizeStr.append("\"DmnSize\":[");
        prunedStr.append("\"Prnd\":[");
        exploredStr.append("\"Explrd\":[");

        for(int i = 0; i < domainSize.length; ) {
            domainSizeStr.append(domainSize[i]);
            prunedStr.append(pruned[i]);
            exploredStr.append(explored[i]);

            ++i;
            if(i != domainSize.length) {
                domainSizeStr.append(", ");
                prunedStr.append(", ");
                exploredStr.append(", ");
            }
        }
        domainSizeStr.append("]");
        prunedStr.append("]");
        exploredStr.append("]");

        str.append(domainSizeStr + ", ");
        str.append(prunedStr + ", ");
        str.append(exploredStr);
        str.append("}");

        return str.toString();
    }
}
