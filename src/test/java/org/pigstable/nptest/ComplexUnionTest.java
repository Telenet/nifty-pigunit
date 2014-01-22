package org.pigstable.nptest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.validator.DataSetValidator;

import static org.pigstable.nptest.validator.DataSetValidator.dataset;
import static org.pigstable.nptest.validator.FieldValidator.isString;
import static org.pigstable.nptest.validator.FieldValidator.string;
import static org.pigstable.nptest.validator.TupleValidator.tuple;

public class ComplexUnionTest {
    private static final String PIG_SCRIPT = "src/test/resources/simpleUnion.pig";

    private NiftyPigTest script;

    @Before
    public void setUp() throws Exception {
        script = new NiftyPigTest(PIG_SCRIPT);
    }

    @Test
    @Category(TestCategories.PigTest.class)
    public void testValidation() throws Exception{
        // -- indicate which data we want to use for which pig aliases
        String[] setA = {
                "139380;AD210",
                "139380;AD2100"
        };
        script.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO",
        };
        script.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        script.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = script.validate(dataset("result").mode(DataSetValidator.ValidationMode.Single).size(4)
                .add(tuple().field(isString()).field(isString()))
        );

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }

    @Test
    @Category(TestCategories.PigTest.class)
    public void testValidationBySequence() throws Exception{
        // -- indicate which data we want to use for which pig aliases
        String[] setA = {
                "139380;AD210",
                "139380;AD2100"
        };
        script.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO",
        };
        script.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        script.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = script.validate(dataset("result").mode(DataSetValidator.ValidationMode.ByOrder).size(4)
                .add(tuple().field(string("SOHO")).field(string("SOHO")))
                .add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")))
                .add(tuple().field(string("139380")).field(string("AD210")))
                .add(tuple().field(string("139380")).field(string("AD2100")))
        );

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }

    @Test
    @Category(TestCategories.PigTest.class)
    public void testValidationBySelectors() throws Exception {
        // -- indicate which data we want to use for which pig aliases
        String[] setA = {
                "139380;AD210",
                "139381;AD2100"
        };
        script.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO",
        };
        script.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        script.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = script.validate(dataset("result").mode(DataSetValidator.ValidationMode.BySelector).size(4)
            .add(tuple().select("139381").field(string("AD2100")))
            .add(tuple().select("SOHO").field(string("SOHO")))
            .add(tuple().select("9xaiqa00840tx05pp0kqi").field(string("SOHO")))
            .add(tuple().select("139380").field(string("AD210")))
        );

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }
}
