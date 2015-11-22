package io.arabesque.utils;

public abstract class BasicFactory<O> implements Factory<O> {
    public BasicFactory() {
        reset();
    }

    @Override
    public void reset() {
        // Empyt on purpose
    }
}
