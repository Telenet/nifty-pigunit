package org.pigstable.nptest.result;

public interface ValidationReport {

    /**
     * Indicate if the validation was a success.
     *
     * @return  true if the validation was successful, false if not
     */
    boolean isValid();

}
