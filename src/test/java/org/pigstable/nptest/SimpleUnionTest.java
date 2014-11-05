package org.pigstable.nptest;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.pigstable.nptest.dataset.TestDataSet;
import org.pigstable.nptest.dataset.ValidatedDataSet;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.validator.DataSetValidator;

import static org.pigstable.nptest.validator.DataSetValidator.dataset;
import static org.pigstable.nptest.validator.FieldValidator.string;
import static org.pigstable.nptest.validator.TupleValidator.tuple;

public class SimpleUnionTest {
    private static final String PIG_SCRIPT = "simpleUnion.pig";

    @Test
    @Category(TestCategories.PigTest.class)
    public void testTextInput() throws Exception{
        // -- initialize the pig testing class
        NiftyPigTest test = new NiftyPigTest(PIG_SCRIPT);

        TestDataSet setA = new TestDataSet(new Schema());

        setA.add("139380;AD210");
        setA.add("139380;AD2100");

        test.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        TestDataSet setB = new TestDataSet(new Schema());

        setB.add("SOHO;SOHO");
        setB.add("9xaiqa00840tx05pp0kqi;SOHO");

        test.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        test.execute();

        ValidatedDataSet validatedDataset = new ValidatedDataSet();

        validatedDataset.add(tuple().field(string("139380")).field(string("AD210")));
        validatedDataset.add(tuple().field(string("139380")).field(string("AD2100")));
        validatedDataset.add(tuple().field(string("SOHO")).field(string("SOHO")));
        validatedDataset.add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")));

        //Validate with new api
        DataSetReport report = test.validate("result", validatedDataset, DataSetValidator.ValidationMode.ByOrder,4);

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }

}
