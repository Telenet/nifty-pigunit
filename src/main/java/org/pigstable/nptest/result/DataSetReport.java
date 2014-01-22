package org.pigstable.nptest.result;

import java.util.ArrayList;
import java.util.List;

public class DataSetReport implements ValidationReport {
    private List<TupleReport> tupleReports = new ArrayList<TupleReport>();
    private String name;
    private String message;
    private boolean hasTupleError = false;

    public DataSetReport(String name) {
        this.name = name;
    }

    public void add(TupleReport report) {
        tupleReports.add(report);
        if (! report.isValid()) hasTupleError = true;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public List<TupleReport> getTupleReports() {
        return tupleReports;
    }

    public String getName() {
        return name;
    }

    public boolean isHasTupleError() {
        return hasTupleError;
    }

    @Override
    public boolean isValid() {
        return message == null && !hasTupleError;
    }
}
