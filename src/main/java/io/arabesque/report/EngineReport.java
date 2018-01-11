package io.arabesque.report;

import java.io.*;

/**
 * Created by ehussein on 10/1/17.
 */
public abstract class EngineReport implements Report {
    public int superstep = 0;
    public long startTime = 0;
    public long endTime = 0;

    public long getRuntime() { return endTime - startTime; }

    protected void ensurePathExists(String path) {
        File file = new File(path);
        if(!file.exists())
            file.mkdirs();
    }

    @Override
    public void saveReport(String path) throws IOException {
        if(superstep == 0) {
            File file = new File(path);
            if(file.exists())
                file.delete();
        }
        else {
            // to remove "]" written at last superstep
            RandomAccessFile raf  = new RandomAccessFile(new File(path), "rw");
            raf.setLength(raf.length() - 1);
            raf.close();
        }

        PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(path,true)));

        if(superstep == 0) {
            pw.print("[");
        }
        else
            pw.print(",");

        pw.print(toString() + "\n");
        pw.print("]");

        pw.close();
    }
}
