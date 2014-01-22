package org.pigstable.nptest.result;

public class FieldReport implements ValidationReport {
    private String message;
    private Throwable throwable;
    private int fieldSequence;

    public FieldReport(int sequence) {
        this.fieldSequence = sequence;
    }

    @Override
    public String toString() {
        return "Field #" + fieldSequence + ": " + message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public int getFieldSequence() {
        return fieldSequence;
    }

    @Override
    public boolean isValid() {
        return message == null && throwable == null;
    }
}
