package io.arabesque.utils;

import net.openhft.koloboke.function.IntConsumer;

import java.io.DataOutput;
import java.io.IOException;

public class IntWriterConsumer implements IntConsumer {
    private DataOutput dataOutput;

    public void setDataOutput(DataOutput dataOutput) {
        this.dataOutput = dataOutput;
    }

    @Override
    public void accept(int i) {
        try {
            dataOutput.writeInt(i);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
