package org.pigstable.nptest;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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

        // -- indicate which data we want to use for which pig aliases
        String[] setA = {
                "139380;AD210",
                "139380;AD2100"
        };
        test.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO",
        };
        test.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        test.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = test.validate(dataset("result").mode(DataSetValidator.ValidationMode.ByOrder).size(4)
                .add(tuple().field(string("139380")).field(string("AD210")))
                .add(tuple().field(string("139380")).field(string("AD2100")))
                .add(tuple().field(string("SOHO")).field(string("SOHO")))
                .add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")))
        );

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }

}
