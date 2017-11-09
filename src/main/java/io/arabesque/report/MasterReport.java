package io.arabesque.report;

import java.io.IOException;
import java.util.ArrayList;
/**
 * Created by ehussein on 10/1/17.
 */
public class MasterReport extends EngineReport {
    public ArrayList<String> storageSummary = new ArrayList<String>();
    public ArrayList<Long> patternSize = new ArrayList<Long>();
    public ArrayList<Long> storageSize = new ArrayList<Long>();
    public ArrayList<Long> domainEntriesCalculatedSize = new ArrayList<Long>();
    public ArrayList<Long> calculatedSize = new ArrayList<Long>();
    public ArrayList<Long> numberOfWordsInDomains = new ArrayList<Long>();
    public ArrayList<Long> numberOfWordsInConnections = new ArrayList<Long>();

    @Override
    public void saveReport(String path) throws IOException {
        ensurePathExists(path);
        String filePath = path + "/MasterReport.txt";
        super.saveReport(filePath);
    }

    @Override
    public String toString() { return toJSONString(); }

    public String toJSONString() {
        StringBuilder str = new StringBuilder();

        str.append("{\"super_step\":" + superstep + ", ");
        str.append("\"runtime\":" + getRuntime() + ", ");
        str.append("\"StorageSummary\":[");

        int i = 0;
        while(i < storageSize.size()) {
            str.append("{");
            str.append("\"Summary\":" + storageSummary.get(i) + ",");
            str.append("\"NumberOfWordsInDomains\":" + numberOfWordsInDomains.get(i) + ",");
            str.append("\"NumberOfWordsInConnections\":" + numberOfWordsInConnections.get(i) + ",");
            str.append("\"PatternSize\":" + patternSize.get(i) + ",");
            str.append("\"StorageSize\":" + storageSize.get(i) + ",");
            str.append("\"DomainEntriesCalculatedSize\":" + domainEntriesCalculatedSize.get(i) + ",");
            str.append("\"CalculatedSize\":" + calculatedSize.get(i));
            str.append("}");

            i += 1;

            if(i != storageSize.size())
                str.append(", ");
        }

        str.append("]");
        str.append("}");

        return str.toString();
    }
}
