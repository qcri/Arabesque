package io.arabesque.odag;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.giraph.utils.ExtendedByteArrayDataOutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by afonseca on 3/14/2015.
 */
public class ODAGPartLZ4Wrapper extends ODAGPartWrapper {
    private static final int MB = 1024 * 1024;

    private boolean uncompressed;
    private int uncompressedSize;

    private LZ4Factory lz4factory;

    public ODAGPartLZ4Wrapper() {
        lz4factory = LZ4Factory.fastestInstance();
        reset();
    }

    @Override
    public void readEzip(ODAG ezip) {
        ensureDecompressed();
        super.readEzip(ezip);
    }

    @Override
    public boolean overThreshold() {
        return super.overThreshold();
    }

    public void ensureDecompressed() {
        decompress();
    }

    public void ensureCompressed() {
        compress();
    }

    @Override
    public void reset() {
        uncompressed = true;
        uncompressedSize = 0;
        super.reset();
    }

    private void compress() {
        if (!uncompressed) {
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
        compress();
        dataOutput.writeInt(uncompressedSize);
        super.write(dataOutput);
    }

    private synchronized void decompress() {
        if (uncompressed) {
            return;
        }

        //LOG.info("Big no no");

        //long start = System.currentTimeMillis();
        LZ4FastDecompressor decompressor = lz4factory.fastDecompressor();

        ByteBuffer dest = ByteBuffer.allocate(uncompressedSize);
        ByteBuffer src = ByteBuffer.wrap(byteArrayOutputCache.getByteArray(), 0, byteArrayOutputCache.getPos());

        decompressor.decompress(src, 0, dest, 0, uncompressedSize);

        byteArrayOutputCache = new ExtendedByteArrayDataOutput(dest.array(), uncompressedSize);
        uncompressed = true;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        uncompressedSize = dataInput.readInt();
        uncompressed = false;
        super.readFields(dataInput);
    }
}