package org.pigstable.nptest.validator;

import org.junit.Assert;
import org.junit.Test;
import org.pigstable.nptest.result.FieldReport;

public class FieldValidatorTest {

    @Test
    public void testValid() {
        FieldValidator validator = FieldValidator.string("garbage");

        FieldReport report = validator.validate(0, "garbage");
        Assert.assertTrue(report.isValid());
    }

    @Test
    public void testInvalid() {
        FieldValidator validator = FieldValidator.string("garbage");

        FieldReport report = validator.validate(0, "-----");
        Assert.assertFalse(report.isValid());
    }
}
