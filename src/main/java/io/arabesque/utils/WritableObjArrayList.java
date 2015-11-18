package io.arabesque.utils;

import io.arabesque.utils.collection.ObjArrayList;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

public abstract class WritableObjArrayList<O extends Writable> extends ObjArrayList<O> implements Writable {
    public WritableObjArrayList() {
    }

    public WritableObjArrayList(int capacity) {
        super(capacity);
    }

    public WritableObjArrayList(Collection<O> elements) {
        super(elements);
    }

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
