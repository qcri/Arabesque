package io.arabesque.cache;

import io.arabesque.conf.Configuration;
import io.arabesque.misc.WritableObject;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LZ4ObjectCache extends ByteArrayObjectCache {
    private static final Logger LOG = Logger.getLogger(LZ4ObjectCache.class);
    private static final int MB = 1024 * 1024;

    private boolean uncompressed;
    private int uncompressedSize;

    private LZ4Factory lz4factory;

    public LZ4ObjectCache() {
        lz4factory = LZ4Factory.fastestInstance();
        reset();
    }

    @Override
    public boolean overThreshold() {
        decompressDataInput();
        return super.overThreshold();
    }

    @Override
    public void addObject(WritableObject object) throws IOException {
        decompressDataInput();
        super.addObject(object);
    }

    @Override
    public void prepareForIteration() {
        decompressDataInput();
        super.prepareForIteration();
    }

    @Override
    public void reset() {
        uncompressed = true;
        uncompressedSize = 0;
        super.reset();
    }

    private void compressDataOutput() {
        if (!Configuration.get().isUseCompressedCaches() || !uncompressed) {
            return;
        }

        uncompressedSize = byteArrayOutputCache.getPos();
        LZ4Compressor lz4Compressor = lz4factory.fastCompressor();
        int maxCompressedLength = lz4Compressor.maxCompressedLength(uncompressedSize);
        ByteBuffer compressed = ByteBuffer.wrap(new byte[maxCompressedLength]);
        int compressedLength = lz4Compressor.compress(ByteBuffer.wrap(byteArrayOutputCache.getByteArray()), 0,
                uncompressedSize, compressed, 0, maxCompressedLength);
        byteArrayOutputCache = new ExtendedByteArrayDataOutput(compressed.array(), compressedLength);
        uncompressed = false;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (Configuration.get().isUseCompressedCaches()) {
            compressDataOutput();
            dataOutput.writeInt(uncompressedSize);
        }
        super.write(dataOutput);
    }

    private void decompressDataInput() {
        if (!Configuration.get().isUseCompressedCaches() || uncompressed) {
            return;
        }

        //LOG.info("Big no no");

        LZ4FastDecompressor decompressor = lz4factory.fastDecompressor();

        ByteBuffer dest = ByteBuffer.allocate(uncompressedSize);
        ByteBuffer src = ByteBuffer.wrap(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());

        decompressor.decompress(src, 0, dest, 0, uncompressedSize);

        byteArrayOutputCache = new ExtendedByteArrayDataOutput(dest.array(), uncompressedSize);
        uncompressed = true;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (Configuration.get().isUseCompressedCaches()) {
            uncompressedSize = dataInput.readInt();
            uncompressed = false;
        } else {
            uncompressedSize = 0;
            uncompressed = true;
        }
        super.readFields(dataInput);
    }
}
