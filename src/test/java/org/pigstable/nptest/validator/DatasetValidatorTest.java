package org.pigstable.nptest.validator;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;

import java.util.Arrays;
import java.util.List;

import static org.pigstable.nptest.validator.DataSetValidator.dataset;
import static org.pigstable.nptest.validator.FieldValidator.*;
import static org.pigstable.nptest.validator.TupleValidator.tuple;

public class DatasetValidatorTest {

    @Test
    public void testOneValidator() throws Exception {
        DataSetValidator validator = dataset("TestDataSet")
                .mode(DataSetValidator.ValidationMode.Single)
                .size(3)
                .add(tuple()
                        .field(any())
                        .field(any())
                        .field(any()))
                .result();

        TupleFactory f = TupleFactory.getInstance();

        List<Tuple> tuples = Arrays.asList(
                f.newTuple(Arrays.asList("r1f1", "r1f2", "r1f3")),
                f.newTuple(Arrays.asList("r2f1", "r2f2", "r2f3")),
                f.newTuple(Arrays.asList("r3f1", "r3f2", "r3f3"))
        );

        DataSetReport report = validator.validate(tuples.iterator());

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }

    @Test
    public void testSortedValidators() throws Exception {
        DataSetValidator validator = dataset("TestDataSet")
                .mode(DataSetValidator.ValidationMode.ByOrder)
                .size(3)
                .add(tuple()
                        .field(string("r1f1"))
                        .field(string("r1f2"))
                        .field(string("r1f3")))
                .add(tuple()
                        .field(string("r2f1"))
                        .field(string("r2f2"))
                        .field(string("r2f3")))
                .add(tuple()
                        .field(string("r3f1"))
                        .field(string("r3f2"))
                        .field(string("r3f3")))
                .result();

        TupleFactory f = TupleFactory.getInstance();

        List<Tuple> tuples = Arrays.asList(
            f.newTuple(Arrays.asList("r1f1", "r1f2", "r1f3")),
            f.newTuple(Arrays.asList("r2f1", "r2f2", "r2f3")),
            f.newTuple(Arrays.asList("r3f1", "r3f2", "r3f3"))
        );

        DataSetReport report = validator.validate(tuples.iterator());

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }

    @Test
    public void testSelectedValidators() throws Exception{
        DataSetValidator validator = dataset("TestDataSet")
                .mode(DataSetValidator.ValidationMode.BySelector)
                .size(3)
                .add(tuple()
                        .select("r3f1")
                        .field(string("r3f2"))
                        .field(string("r3f3")))
                .add(tuple()
                        .select("r1f1")
                        .field(string("r1f2"))
                        .field(string("r1f3")))
                .add(tuple()
                        .select("r2f1")
                        .field(string("r2f2"))
                        .field(string("r2f3")))
                .result();

        TupleFactory f = TupleFactory.getInstance();

        List<Tuple> tuples = Arrays.asList(
                f.newTuple(Arrays.asList("r1f1", "r1f2", "r1f3")),
                f.newTuple(Arrays.asList("r2f1", "r2f2", "r2f3")),
                f.newTuple(Arrays.asList("r3f1", "r3f2", "r3f3"))
        );

        DataSetReport report = validator.validate(tuples.iterator());

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }


    @Test
    public void testSizeOnlyValidators() throws Exception{
        DataSetValidator validator = dataset("TestDataSet")
                .mode(DataSetValidator.ValidationMode.SizeOnly)
                .size(3).result();

        TupleFactory f = TupleFactory.getInstance();

        List<Tuple> tuples = Arrays.asList(
                f.newTuple(Arrays.asList("r1f1", "r1f2", "r1f3")),
                f.newTuple(Arrays.asList("r2f1", "r2f2", "r2f3")),
                f.newTuple(Arrays.asList("r3f1", "r3f2", "r3f3"))
        );

        DataSetReport report = validator.validate(tuples.iterator());

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }
}
