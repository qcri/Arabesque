package io.arabesque.report;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by ehussein on 10/1/17.
 */
public class PartitionReport extends EngineReport {
    public int partitionId = 0;
    public StorageReport[] storageReports = null;//: ArrayBuffer[StorageReport] = new ArrayBuffer[StorageReport]();

    // num of actual embeddings stored in the storage
    public BigInteger cummAct = BigInteger.ZERO;
    // Total number of enumerations the storage could hold
    public BigInteger cummEnums = BigInteger.ZERO;
    // used to show load balancing between partitions
    public BigInteger cummCmplt = BigInteger.ZERO;
    // how many invalid embeddings this storage/partition generated
    public BigInteger cummSprs = BigInteger.ZERO;

    @Override
    public void saveReport(String path) throws IOException {
        ensurePathExists(path);
        String filePath = "" + path + "/Partition" + partitionId + "Report.txt";
        super.saveReport(filePath);
    }

    @Override
    public String toString() { return toJSONString(); }

    public String toJSONString() {
        calcCummulatives();

        StringBuilder str = new StringBuilder();

        str.append("{\"super_step\":" + superstep + ", ");
        str.append("\"start_time\":" + this.startTime + ", ");
        str.append("\"end_time\":" + this.endTime + ", ");
        str.append("\"runtime\":" + getRuntime() + ", ");
        str.append("\"TotalEnums\":" + this.cummEnums + ", ");
        str.append("\"TotalActual\":" + this.cummAct + ", ");
        str.append("\"TotalCmplt\":" + this.cummCmplt + ", ");
        str.append("\"TotalSprs\":" + this.cummSprs + ", ");

        str.append("\"StorageReports\":[");

        for(int i = 0; i < storageReports.length;) {
            storageReports[i].storageId = i;
            str.append(storageReports[i]);
            i += 1;

            if(i != storageReports.length)
                str.append(", ");
        }

        str.append("]");
        str.append("}");

        return str.toString();
    }

    private void calcCummulatives() {
        cummEnums = BigInteger.ZERO;
        cummAct = BigInteger.ZERO;
        cummCmplt = BigInteger.ZERO;
        cummSprs = BigInteger.ZERO;

        for(int i = 0; i < storageReports.length; ++i) {
            cummEnums = cummEnums.add(BigInteger.valueOf(storageReports[i].numEnumerations));
            cummAct = cummAct.add(BigInteger.valueOf(storageReports[i].numActualEmbeddings));
            cummCmplt = cummCmplt.add(BigInteger.valueOf(storageReports[i].numCompleteEnumerationsVisited));
            cummSprs = cummSprs.add(BigInteger.valueOf(storageReports[i].numSpuriousEmbeddings));
        }
    }
}
