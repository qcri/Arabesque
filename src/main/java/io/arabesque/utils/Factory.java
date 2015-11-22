package io.arabesque.utils;

public interface Factory<O> {
    O createObject();
    void reset();
}
