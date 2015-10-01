package io.arabesque.computation.comm;

import io.arabesque.conf.Configuration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MessageWrapper implements Writable {
    private Writable message;

    public MessageWrapper() {
    }

    public MessageWrapper(Writable message) {
        this.message = message;
    }

    public <M extends Writable> M getMessage() {
        return (M) message;
    }

    public void setMessage(Writable message) {
        this.message = message;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        message.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        message = Configuration.get().createCommunicationStrategyMessage();

        message.readFields(dataInput);
    }
}
