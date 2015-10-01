package io.arabesque.odag;

import io.arabesque.conf.Configuration;
import io.arabesque.pattern.Pattern;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODAGPartWrapper implements Writable {
    private static final Logger LOG =
            Logger.getLogger(ODAGPartWrapper.class);

    protected Pattern pattern;
    protected int partId;
    protected ExtendedByteArrayDataOutput byteArrayOutputCache;

    public ODAGPartWrapper() {
        initialize();
    }

    private void initialize() {
        byteArrayOutputCache = new ExtendedByteArrayDataOutput();
        reset();
    }

    public void setPartId(int partId) {
        this.partId = partId;
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public void setByteArrayOutputCache(ExtendedByteArrayDataOutput byteArrayOutputCache) {
        this.byteArrayOutputCache = byteArrayOutputCache;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public int getPartId() {
        return partId;
    }

    public ExtendedByteArrayDataOutput getByteArrayOutputCache() {
        return byteArrayOutputCache;
    }

    public void readEzip(ODAG ezip) {
        ExtendedByteArrayDataInput byteArrayInputCache = null;
        try {
            byteArrayInputCache = new ExtendedByteArrayDataInput(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());
            //LOG.info(byteArrayInputCache.available());
            ezip.setPattern(pattern);
            ezip.readFields(byteArrayInputCache);
        } catch (IOException e) {
            LOG.info("pattern: " + pattern);
            LOG.info("partId: " + partId);
            LOG.info("ezip: " + ezip);
            LOG.info("input.available: " + byteArrayInputCache.available());
            LOG.info("input.pos: " + byteArrayInputCache.getPos());
            LOG.info("output.pos: " + byteArrayOutputCache.getPos());
            throw new RuntimeException(e);
        }
    }

    public void writeEzip(ODAG ezip, int partId) throws IOException {
        reset();
        if (byteArrayOutputCache == null) {
            byteArrayOutputCache = new ExtendedByteArrayDataOutput();
        }
        this.partId = partId;
        pattern = ezip.getPattern();
        ezip.write(byteArrayOutputCache);
    }

    public boolean isEmpty() {
        return byteArrayOutputCache.getPos() == 0;
    }

    public boolean overThreshold() {
        return byteArrayOutputCache.getPos() > Configuration.get().getCacheThresholdSize();
    }

    public void write(DataOutput dataOutput) throws IOException {
        pattern.write(dataOutput);
        dataOutput.writeInt(partId);
        dataOutput.writeInt(byteArrayOutputCache.getPos());
        dataOutput.write(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (pattern == null) {
            pattern = Configuration.get().createPattern();
        }

        pattern.readFields(dataInput);

        partId = dataInput.readInt();

        int size = dataInput.readInt();

        byteArrayOutputCache.reset();
        byteArrayOutputCache.skipBytes(size);
        dataInput.readFully(byteArrayOutputCache.getByteArray(), 0, size);
    }

    public void reset() {
        if (byteArrayOutputCache != null) {
            byteArrayOutputCache.reset();
        }
        partId = -1;
    }

    @Override
    public String toString() {
        return "ODAGPartWrapper{" +
                debugCache() +
                '}' + super.toString();
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
