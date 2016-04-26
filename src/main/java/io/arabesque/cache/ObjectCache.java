package io.arabesque.cache;

import io.arabesque.misc.WritableObject;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.Externalizable;

/**
 * Created by afonseca on 3/14/2015.
 */
public interface ObjectCache extends Writable, Externalizable {
    boolean hasNext();

    WritableObject next();

    void addObject(WritableObject object) throws IOException;

    boolean isEmpty();
}
