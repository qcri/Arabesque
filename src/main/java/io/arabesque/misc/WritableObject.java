package io.arabesque.misc;

import org.apache.hadoop.io.Writable;

public interface WritableObject extends Writable {
    String toOutputString();
}
