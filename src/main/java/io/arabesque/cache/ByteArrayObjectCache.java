package io.arabesque.cache;

import io.arabesque.conf.Configuration;
import io.arabesque.misc.WritableObject;
import org.apache.giraph.utils.ExtendedByteArrayDataInput;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class ByteArrayObjectCache implements ObjectCache {
    protected ExtendedByteArrayDataOutput byteArrayOutputCache;
    protected ByteArrayObjectCacheIterator byteArrayObjectCacheIterator;

    public static class ByteArrayObjectCacheIterator
            implements Iterator<WritableObject> {
        protected WritableObject reusableObject = null;
        protected ExtendedByteArrayDataInput byteArrayInputCache;
        protected Configuration configuration;

        public ByteArrayObjectCacheIterator(ByteArrayObjectCache objectCache) {
            ExtendedByteArrayDataOutput byteArrayOutputCache = objectCache.byteArrayOutputCache;
            byteArrayInputCache = new ExtendedByteArrayDataInput(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());
            configuration = Configuration.get();
        }

        @Override
        public boolean hasNext() {
            return !byteArrayInputCache.endOfInput();
        }

        @Override
        public WritableObject next() {
            if (reusableObject == null) {
                reusableObject = configuration.createEmbedding();
            }

            try {
                reusableObject.readFields(byteArrayInputCache);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return reusableObject;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "ByteArrayObjectCacheIterator{" +
                    "byteArrayInputCache=" + byteArrayInputCache +
                    ", reusableObject=" + reusableObject +
                    '}';
        }
    }

    public ExtendedByteArrayDataOutput getByteArrayOutputCache() {
        return byteArrayOutputCache;
    }

    public ByteArrayObjectCache() {
        initialize();
    }

    private void initialize() {
        byteArrayOutputCache = new ExtendedByteArrayDataOutput();
    }

    @Override
    public boolean hasNext() {
        return byteArrayObjectCacheIterator.hasNext();
    }

    public void prepareForIteration() {
        byteArrayObjectCacheIterator = new ByteArrayObjectCacheIterator(this);
    }

    @Override
    public WritableObject next() {
        return byteArrayObjectCacheIterator.next();
    }

    @Override
    public void addObject(WritableObject object) throws IOException {
        object.write(byteArrayOutputCache);
    }

    @Override
    public boolean isEmpty() {
        return byteArrayOutputCache.getPos() == 0;
    }

    public boolean overThreshold() {
        return byteArrayOutputCache.getPos() > Configuration.get().getCacheThresholdSize();
    }

    @Override
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
        return "ObjectCache{" +
                debugCache() +
                '}';
    }

    public String debugCache() {
        StringBuilder sb = new StringBuilder();
        sb.append("bigDataOutputCache: ");
        sb.append(byteArrayOutputCache);
        if (byteArrayOutputCache != null) {
            sb.append('\n');
            sb.append("bigDataOutputCache.pos: ");
            sb.append(byteArrayOutputCache.getPos());
        }
        sb.append('\n');
        sb.append("iterator: ");
        sb.append(byteArrayObjectCacheIterator);

        return sb.toString();
    }
}
