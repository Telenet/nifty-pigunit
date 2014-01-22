package org.pigstable.nptest.validator;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;
import org.pigstable.nptest.result.TupleReport;

import java.util.Arrays;

public class TupleValidatorTest {

    @Test
    public void testApplicableWhenNoSelectors() {
        TupleValidator validator = TupleValidator.tuple()
                .field(FieldValidator.any())
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        Assert.assertTrue(validator.isApplicable(tuple));
    }

    @Test
    public void testApplicableWhenOneSelectors() {
        TupleValidator validator = TupleValidator.tuple()
                .select("key")
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        Assert.assertTrue(validator.isApplicable(tuple));
    }

    @Test
    public void testApplicableWhenMultipleSelectors() {
        TupleValidator validator = TupleValidator.tuple()
                .select("key-1")
                .select("key-2")
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key-1",
                "key-2",
                "value"
        ));

        Assert.assertTrue(validator.isApplicable(tuple));
    }

    @Test
    public void testNotApplicable() {
        TupleValidator validator = TupleValidator.tuple()
                .select("garbage")
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        Assert.assertFalse(validator.isApplicable(tuple));
    }

    @Test
    public void testValid() {
        TupleValidator validator = TupleValidator.tuple()
                .field(FieldValidator.any())
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        TupleReport report = validator.validate("TST", tuple);
        Assert.assertTrue(report.isValid());
    }

    @Test
     public void testValidWithOneSelector() {
        TupleValidator validator = TupleValidator.tuple()
                .select("key")
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        TupleReport report = validator.validate("TST", tuple);
        Assert.assertTrue(report.isValid());
    }

    @Test
    public void testInvalidField() {
        TupleValidator validator = TupleValidator.tuple()
                .field(FieldValidator.string("garbage"))
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        TupleReport report = validator.validate("TST", tuple);
        Assert.assertFalse(report.isValid());
        Assert.assertTrue(report.hasFieldError());
    }

    @Test
    public void testInvalidLength() {
        TupleValidator validator = TupleValidator.tuple()
                .field(FieldValidator.any())
                .field(FieldValidator.any())
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        TupleReport report = validator.validate("TST", tuple);
        Assert.assertFalse(report.isValid());
    }

    @Test
    public void testInvalidLengthWithOneSelector() {
        TupleValidator validator = TupleValidator.tuple()
                .select("key")
                .field(FieldValidator.any())
                .field(FieldValidator.any())
                .result();

        Tuple tuple = TupleFactory.getInstance().newTuple(Arrays.asList(
                "key",
                "value"
        ));

        TupleReport report = validator.validate("TST", tuple);
        Assert.assertFalse(report.isValid());
    }
}
