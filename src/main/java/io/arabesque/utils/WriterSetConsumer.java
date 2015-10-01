package io.arabesque.utils;

import net.openhft.koloboke.function.IntConsumer;

import java.io.DataOutput;
import java.io.IOException;

public class WriterSetConsumer implements IntConsumer {
    DataOutput dataOutput;

    @Override
    public void accept(int i) {
        try {
            dataOutput.writeInt(i);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Writing failed");
        }
    }

    public void setOutput(DataOutput dataOutput) {
        this.dataOutput = dataOutput;
    }
}
