package io.arabesque.report;

import java.io.IOException;

/**
 * Created by ehussein on 10/1/17.
 */
public interface Report {
    void saveReport(String path) throws IOException;
}
