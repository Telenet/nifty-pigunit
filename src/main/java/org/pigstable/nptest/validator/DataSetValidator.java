package org.pigstable.nptest.validator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.pig.data.Tuple;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.result.TupleReport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataSetValidator {
    public static enum ValidationMode { Single, ByOrder, BySelector, SizeOnly }

    private String name;
    private ValidationMode validationMode;
    private Integer expectedSize;
    private List<TupleValidator> tupleValidators;

    public DataSetValidator() { }

    /**
     * Create a new Builder for a DataSetValidator.
     *
     * @param name  the name of the dataset to validate
     * @return a Builder used to create the DataSetValidator
     */
    public static Builder dataset(String name) {
        return new Builder(name);
    }

    /**
     * Validate the given collection of Tuples.
     *
     *  For each tuple a tuple validator will be retrieved based on the tuples sequence in the bag. If only
     *  a single tupleValidator has been declared that one will be used for all tuples in the bag.
     *
     * @param actual    the collection of tuples to validate.
     */
    @SuppressWarnings("unchecked")
    public DataSetReport validate(Iterator<Tuple> actual) {
        DataSetReport report = new DataSetReport(this.name);

        // -- check there are validators available
        if (this.tupleValidators.isEmpty() && !this.getValidationMode().equals(ValidationMode.SizeOnly))
            throw new RuntimeException("No tuple validators have been defined!");

        List<Tuple> tuples = IteratorUtils.toList(actual);
        if (tuples.size() != expectedSize) {
            report.setMessage(String.format("Expected %s tuples but got %s of them", expectedSize, tuples.size()));
        }

        switch (validationMode) {
            case BySelector:
                validateBySelector(report, tuples);
                break;

            case ByOrder:
                if (tuples.size() != tupleValidators.size()) throw new RuntimeException(
                        "For validation ByOrder to work the number of validators must match the number of tuples. " +
                        "Right now there are " + tupleValidators.size() + " validators while there are " +
                        tuples.size()  + " tuples"
                );

                validateByOrder(report, tuples);
                break;

            case Single:
                if (tupleValidators.size() != 1) throw new RuntimeException(
                        "For Single validation to work the number of validators must be one (1). " +
                        "Right now there are " + tupleValidators.size() + " validators"
                );

                validateBySingleValidator(report, tuples);
                break;

            case SizeOnly:
                if (tuples.size() != expectedSize) throw new RuntimeException(String.format("Expect tuple size was %s, Tuple size received %s",expectedSize,tuples.size()));
                break;
        }

        return report;
    }

    protected void validateBySingleValidator(DataSetReport report, List<Tuple> actual) {
        TupleValidator validator = this.tupleValidators.get(0);

        for (int i = 0; i < actual.size(); i++) {
            report.add(validator.validate("#" + i, actual.get(i)));
        }
    }
/**
    protected void validateSizeOnly(DataSetReport report, List<Tuple> actual) {
        TupleValidator validator = this.tupleValidators.get(0);

            report.add(validator.validate);
        }
    }

 **/
    protected void validateByOrder(DataSetReport report, List<Tuple> actual) {
        for (int i = 0; i < actual.size(); i++) {
            TupleValidator validator = this.tupleValidators.get(i);
            System.out.println(tupleValidators.get(i) +"|"+actual.get(i));
            report.add(validator.validate("#" + i, actual.get(i)));
        }

    }

    protected void validateBySelector(DataSetReport report, List<Tuple> actual) {
        for (int i = 0; i < actual.size(); i++) {
            Tuple tuple  = actual.get(i);

            // -- create a list of the validators which can be applied to the current tuple
            List<TupleValidator> applicableValidators = new ArrayList<TupleValidator>();
            for (TupleValidator validator : tupleValidators) {
                if (validator.isApplicable(tuple)) applicableValidators.add(validator);
            }

            // -- if more then one validator is applicable, we will fail the record since it smells like a user error
            if (applicableValidators.size() > 1) {
                TupleReport tupleReport = new TupleReport("#" + i);

                // -- construct the message
                StringBuilder msg = new StringBuilder();
                msg.append("Multiple validators are applicable for this tuple: \n");
                for (TupleValidator v : applicableValidators)
                    msg.append("\t- ").append(v).append("\n");

                tupleReport.setMessage(msg.toString());
                report.add(tupleReport);
            } else if (applicableValidators.isEmpty()) {
                TupleReport tupleReport = new TupleReport("#" + i);
                tupleReport.setMessage("No validator could be found for the current record");
                report.add(tupleReport);
            } else {
                report.add(applicableValidators.get(0).validate("#" + i, tuple));
            }
        }
    }

    public String getName() {
        return name;
    }

    public Integer getExpectedSize() {
        return expectedSize;
    }

    public ValidationMode getValidationMode() {
        return validationMode;
    }

    public static class Builder {
        private DataSetValidator validator;

        public Builder(String name) {
            validator = new DataSetValidator();
            validator.name = name;
            validator.tupleValidators = new ArrayList<TupleValidator>();
        }

        /**
         * Set the expected number of tuples inside the dataset.
         *
         * @param expectedSize  a positive number indicating the number of tuples in the dataset
         * @return a builder used to create the DataSetValidator
         */
        public Builder size(int expectedSize) {
            validator.expectedSize = expectedSize;

            return this;
        }

        /**
         * Set the validation mode.
         *
         * @param mode the validation mode
         * @return  a builder used to create the DataSetValidator
         */
        public Builder mode(ValidationMode mode) {
            validator.validationMode = mode;

            return this;
        }

        /**
         * Add a tuple validator.
         *
         * @param tupleValidatorBuilder    the tuple validator to add
         * @return a Builder used to create the DataSetValidator
         */
        public Builder add(TupleValidator.Builder tupleValidatorBuilder) {
            TupleValidator tupleValidator = tupleValidatorBuilder.result();

            // -- I commented this out since we want to leave the responsibility to the user for now.
            /*if (tupleValidator.hasSelectors()) {
                if ((this.validator.validationMode != null) &&
                    (this.validator.validationMode != ValidationMode.BySelector)) {
                    throw new RuntimeException(
                            "This DatasetValidator is already validating in mode " + this.validator.validationMode +
                            ". It is not possible to change it to " + ValidationMode.BySelector
                    );
                } else {
                    this.validator.validationMode = ValidationMode.BySelector;
                }
            }*/

            //  -- check if the validator is valid against the current validation mode
            if (tupleValidator.hasSelectors() && validator.validationMode != ValidationMode.BySelector)
                throw new RuntimeException(
                        "Adding a TupleValidator with selectors is not allowed when the DataSetValidator is in " + validator.validationMode + " mode."
                );

            validator.tupleValidators.add(tupleValidator);

            return this;
        }

        /**
         * Build the DataSetValidator.
         *
         * @return  the DataSetValidator instance we built using this builder.
         */
        public DataSetValidator result() {
            // -- check all fields are set.
            if (validator.name == null) throw new RuntimeException("The validator name has not been set!");
            if (validator.expectedSize == null) throw new RuntimeException("The expected number of tuples has not been set!");
            if (validator.validationMode == null) throw new RuntimeException("The validator validation mode has not been set!");
            if (validator.tupleValidators.isEmpty() && !validator.getValidationMode().equals(ValidationMode.SizeOnly)) throw new RuntimeException("The validator has no tuple validators!");

            return validator;
        }

    }
}
