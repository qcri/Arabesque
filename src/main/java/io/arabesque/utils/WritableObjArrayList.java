package io.arabesque.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class WritableObjArrayList<O extends Writable> extends ObjArrayList<O> implements Writable {
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(size());

        for (O element : this) {
            element.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clear();

        int numElements = dataInput.readInt();

        ensureCapacity(numElements);

        for (int i = 0; i < numElements; ++i) {
            O object = createObject();
            object.readFields(dataInput);
            add(object);
        }
    }

    protected abstract O createObject();
}
