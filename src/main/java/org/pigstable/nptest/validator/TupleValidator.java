package org.pigstable.nptest.validator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.pigstable.nptest.FieldObject;
import org.pigstable.nptest.result.FieldReport;
import org.pigstable.nptest.result.TupleReport;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * A validator for tuples.
 */
public class TupleValidator {
    private List<FieldObject<Object>> selectors;
    private List<FieldObject<FieldValidator>> validators;

    /**
     * Create a builder.
     *
     * @return  a builder to create the TupleValidator
     */
    public static Builder tuple() {
        return new Builder();
    }

    protected TupleValidator() {
        selectors = new LinkedList<FieldObject<Object>>();
        validators = new ArrayList<FieldObject<FieldValidator>>();
    }

    public String toString() {
        StringBuilder result = new StringBuilder();

        result.append("(");
        boolean first = true;
        for (FieldObject selector : selectors) {
            if (first) first = false;
            else result.append(", ");

            result.append(selector.getFieldSequence()).append(":").append(selector.getObject());
        }

        result.append(") => (");

        first = true;
        for (FieldObject validator : validators) {
            if (first) first = false;
            else result.append(", ");

            result.append(validator.getFieldSequence()).append(":").append(validator.getObject());
        }

        result.append(")");

        return result.toString();
    }

    public boolean isApplicable(Tuple tuple) {
        for (FieldObject<Object> selector : selectors) {
            try {
                Object fieldValue = tuple.get(selector.getFieldSequence());
                if (! selector.getObject().equals(fieldValue)) return false;
            } catch (ExecException e) {
                throw new RuntimeException("Unable to get the field with sequence " + selector.getFieldSequence() + " from tuple " + tuple);
            }
        }

        return true;
    }

    /**
     * Validate the given Tuple.
     *
     *  The validator will run through each field of the tuple and call its field validator. The
     *  field validator will actually determine if the field is valid or not
     *
     * @param key   the key to identify the tuple
     * @param tuple the tuple to validate
     * @return null if the given tuple is valid, the tuple error if it is not.
     */
    public TupleReport validate(String key, Tuple tuple) {
        TupleReport report = new TupleReport(key);

        // -- check the length
        if (tuple.size() != (validators.size() + selectors.size())) {
            report.setMessage("Invalid tuple size " + tuple.size() + " while " + (validators.size() + selectors.size()) + " were expected");
            return report;
        }

        for (FieldObject<FieldValidator> validator : validators) {
            int seq = validator.getFieldSequence();
            FieldValidator fieldValidator = validator.getObject();

            try {
                report.add(fieldValidator.validate(seq, tuple.get(seq)));
            } catch (ExecException ee) {
                FieldReport fieldReport = new FieldReport(validator.getFieldSequence());
                fieldReport.setMessage("Unable to retrieve the value for field #" + validator.getFieldSequence());
                fieldReport.setThrowable(ee);
                report.add(fieldReport);
            }
        }

        return report;
    }

    public boolean hasSelectors() {
        return !this.selectors.isEmpty();
    }

    public static class Builder {
        private TupleValidator result;
        private int fieldSequence = 0;

        public Builder() {
            this.result = new TupleValidator();
        }

        /**
         * Add a field validator.
         *
         * @param validator the validator to add
         * @return the builder to create the TupleValidator
         */
        public Builder field(FieldValidator validator) {
            result.validators.add(new FieldObject<FieldValidator>(fieldSequence++, validator));

            return this;
        }

        /**
         * Add a record selector.
         *
         *  Record selectors are used to determine on which record the validation should occur. If no selector
         *  has been defined, the record selector will fall back to sequence based selection.
         *
         * @param selector the selector to add
         * @return the builder to create the TupleValidator
         */
        public Builder select(Object selector) {
            this.result.selectors.add(new FieldObject<Object>(fieldSequence++, selector));

            return this;
        }

        /**
         * Build the tuple validator.
         *
         * @return a TupleValidator instance
         */
        public TupleValidator result() {
            return result;
        }
    }
}
