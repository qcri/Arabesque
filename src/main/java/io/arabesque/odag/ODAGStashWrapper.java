package io.arabesque.odag;

import io.arabesque.conf.Configuration;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by afonseca on 6/17/2015.
 */
public class ODAGStashWrapper implements Writable {
    private static final Logger LOG =
            Logger.getLogger(ODAGStashWrapper.class);

    protected ExtendedByteArrayDataOutput byteArrayOutputCache;

    public ODAGStashWrapper() {
        initialize();
    }

    private void initialize() {
        byteArrayOutputCache = new ExtendedByteArrayDataOutput();
    }

    public void readStash(ODAGStash stashToReadTo) {
        ExtendedByteArrayDataInput byteArrayInputCache = null;
        try {
            byteArrayInputCache = new ExtendedByteArrayDataInput(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());
            LOG.info(byteArrayInputCache.available());
            stashToReadTo.readFields(byteArrayInputCache);
        } catch (IOException e) {
            LOG.info("stashToReadTo: " + stashToReadTo);
            LOG.info("input.available: " + byteArrayInputCache.available());
            LOG.info("input.pos: " + byteArrayInputCache.getPos());
            LOG.info("output.pos: " + byteArrayOutputCache.getPos());
            throw new RuntimeException(e);
        }
    }

    public void writeStash(ODAGStash stashToWrite) throws IOException {
        reset();
        stashToWrite.write(byteArrayOutputCache);
    }

    public boolean isEmpty() {
        return byteArrayOutputCache.getPos() == 0;
    }

    public boolean overThreshold() {
        return byteArrayOutputCache.getPos() > Configuration.get().getCacheThresholdSize();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(byteArrayOutputCache.getPos());
        dataOutput.write(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();

        byteArrayOutputCache.reset();
        byteArrayOutputCache.skipBytes(size);
        dataInput.readFully(byteArrayOutputCache.getByteArray(), 0, size);
    }

    public void reset() {
        byteArrayOutputCache.reset();
    }

    @Override
    public String toString() {
        return "ODAGStashWrapper{" +
                debugCache() +
                '}';
    }

    public String debugCache() {
        StringBuilder sb = new StringBuilder();
        if (byteArrayOutputCache != null) {
            sb.append('\n');
            sb.append("bigDataOutputCache.pos: ");
            sb.append(byteArrayOutputCache.getPos());
        }

        return sb.toString();
    }
}