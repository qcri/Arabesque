package io.arabesque.odag.domain;

import io.arabesque.embedding.Embedding;
import io.arabesque.report.StorageReport;

import java.util.Iterator;

public interface StorageReader extends Iterator<Embedding> {
    void close();

    //public StorageReport getStorageReport();
}
