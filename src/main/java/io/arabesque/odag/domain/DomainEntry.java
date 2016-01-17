package io.arabesque.odag.domain;

import io.arabesque.utils.WriterSetConsumer;
import net.openhft.koloboke.collect.IntCursor;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public interface DomainEntry {
    boolean insertConnectionToWord(int word);

    void aggregate(DomainEntry otherDomainEntry);

    long getCounter();

    void setCounter(long i);

    IntCursor getPointersCursor();

    void incrementCounter(long counter);

    void write(DataOutput dataOutput, WriterSetConsumer writerSetConsumer) throws IOException;

    int getNumPointers();

    int getWastedPointers();

    void incrementCounterFrom(Map<Integer, DomainEntry> followingEntryMap);
}
