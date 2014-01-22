package org.pigstable.nptest.validator;

import org.pigstable.nptest.result.FieldReport;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public abstract class FieldValidator {

    /**
     * Validate the given field value.
     *
     *  Use the JUnit Assert methods to actually validate the field data.
     *
     * @param fieldSequence the sequence of the field in the tuple
     * @param fieldValue the value to validate
     *
     * @return 'null' if the field is valid, the error if it was invalid
     */
    public abstract FieldReport validate(int fieldSequence, Object fieldValue);

    /**
     * The value may be anything, also null.
     *
     * @return the FieldValidator
     */
    public static FieldValidator any() {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) { return new FieldReport(fieldSequence); }

            @Override
            public String toString() { return "any()"; }
        };
    }

    /**
     * The value may be anything except 'null'.
     *
     * @deprecated use isNotNull instead
     * @return the FieldValidator
     */
    public static FieldValidator anyButNull() {
        return isNotNull();
    }

    /**
     * The value must be null, nothing else.
     *
     * @return the FieldValidator
     */
    public static FieldValidator isNull() {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue != null)
                    report.setMessage("The value was not null while we expected it to be null");

                return report;
            }

            @Override
            public String toString() { return "isNull()"; }
        };
    }

    /**
     * The value may be anything except 'null'.
     *
     * @return the FieldValidator
     */
    public static FieldValidator isNotNull() {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null)
                    report.setMessage("The value was null while we expected anything but null");

                return report;
            }

            @Override
            public String toString() { return "isNotNull()"; }
        };
    }

    /**
     * The value must be a string
     *
     * @return the FieldValidator
     */
    public static FieldValidator isString() {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null) {
                    report.setMessage("The value was null while we expected it not to be null");
                    return report;
                }

                if (! (fieldValue instanceof String)) {
                    report.setMessage("The value was not a String while we expected it to be one");
                    return report;
                }

                return report;
            }

            @Override
            public String toString() { return "isString()"; }
        };
    }

    /**
     * The value must be a number
     *
     * @return the FieldValidator
     */
    public static FieldValidator isNumber() {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null) {
                    report.setMessage("The value was null while we expected it not to be null");
                    return report;
                }

                if (! (fieldValue instanceof String)) {
                    report.setMessage("The value was null while we expected it to be a Number");
                    return report;
                }

                return report;
            }

            @Override
            public String toString() { return "isNumber()"; }
        };
    }

    /**
     * The value must equal to the given string.
     *
     *  This validator is case-sensitive.
     *
     * @param expected  the string to match
     * @return the FieldValidator
     */
    public static FieldValidator string(final String expected) {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null) {
                    report.setMessage("The value was null while we expected it not to be null");
                    return report;
                }

                if (! expected.equals(fieldValue.toString())) {
                    report.setMessage("The value was not '" + expected + "' but '" + fieldValue + "'");
                    return report;
                }

                return report;
            }

            @Override
            public String toString() { return "string('" + expected + "')"; }
        };
    }

    /**
     * The value must be the same as the given integer.
     *
     * @param expected  the number to match
     * @return the FieldValidator
     */
    public static FieldValidator number(final Integer expected) {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null) {
                    report.setMessage("The value was null while we expected it not to be null");
                    return report;
                }

                try {
                    Integer actual = Integer.parseInt(fieldValue.toString());

                    if (! expected.equals(actual)) {
                        report.setMessage("The value was not " + expected + " but " + actual);
                        return report;
                    }

                } catch (NumberFormatException nfe) {
                    report.setMessage(nfe.getMessage());
                    report.setThrowable(nfe);
                    return report;
                }

                return report;
            }

            @Override
            public String toString() { return "number('" + expected + "')"; }
        };
    }

    /**
     * The value must be the same as the given integer.
     *
     * @param expected  the number to match
     * @return the FieldValidator
     */
    public static FieldValidator number(final Float expected) {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null) {
                    report.setMessage("The value was null while we expected it not to be null");
                    return report;
                }

                try {
                    Float actual = Float.parseFloat(fieldValue.toString());

                    if (! expected.equals(actual)) {
                        report.setMessage("The value was not " + expected + " but " + actual);
                        return report;
                    }

                } catch (NumberFormatException nfe) {
                    report.setMessage(nfe.getMessage());
                    report.setThrowable(nfe);
                    return report;
                }

                return report;
            }

            @Override
            public String toString() { return "number('" + expected + "')"; }
        };
    }

    /**
     * The value must match the given regular expression.
     *
     * @param regex the regular expression to match
     * @return the FieldValidator
     */
    public static FieldValidator regex(final String regex) {
        return new FieldValidator() {
            @Override
            public FieldReport validate(int fieldSequence, Object fieldValue) {
                FieldReport report = new FieldReport(fieldSequence);

                if (fieldValue == null) {
                    report.setMessage("The value was null while we expected it not to be null");
                    return report;
                }

                try {
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(fieldValue.toString());

                    if (! matcher.find()) {
                        report.setMessage("The value " + fieldValue + " did not match the regex " + regex);
                        return report;
                    }

                } catch (PatternSyntaxException nfe) {
                    report.setMessage(nfe.getMessage());
                    report.setThrowable(nfe);
                    return report;
                }

                return report;
            }

            @Override
            public String toString() { return "regex('" + regex + "')"; }
        };
    }
}
