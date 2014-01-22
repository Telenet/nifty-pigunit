package org.pigstable.nptest.result;

import java.util.ArrayList;
import java.util.List;

public class TupleReport implements ValidationReport {
    private List<FieldReport> fieldReports = new ArrayList<FieldReport>();
    private String key;
    private String message;
    private boolean hasFieldError = false;

    public TupleReport(String key) {
        this.key = key;
    }

    public void add(FieldReport report) {
        fieldReports.add(report);
        if (! report.isValid()) hasFieldError = true;
    }

    public List<FieldReport> getFieldReports() {
        return fieldReports;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public boolean hasFieldError() {
        return hasFieldError;
    }

    public String getKey() {
        return key;
    }

    @Override
    public boolean isValid() {
        return message == null && !hasFieldError;
    }
}
