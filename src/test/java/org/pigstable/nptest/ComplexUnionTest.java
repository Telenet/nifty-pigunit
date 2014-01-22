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
                "1234;Garbage",
                "12345;Collector"
        };
        script.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "Starship;Enterprise",
                "Battlestar;Galactica",
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
                "1234;Garbage",
                "12345;Collector"
        };
        script.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "Starship;Enterprise",
                "Battlestar;Galactica",
        };
        script.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        script.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = script.validate(dataset("result").mode(DataSetValidator.ValidationMode.ByOrder).size(4)
                .add(tuple().field(string("Starship")).field(string("Enterprise")))
                .add(tuple().field(string("Battlestar")).field(string("Galactica")))
                .add(tuple().field(string("1234")).field(string("Garbage")))
                .add(tuple().field(string("12345")).field(string("Collector")))
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
                "1234;Garbage",
                "12345;Collector"
        };
        script.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        String[] setB = {
                "Starship;Enterprise",
                "Battlestar;Galactica",
        };
        script.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        script.execute();

        // -- validate the output using the DataSetValidator
        DataSetReport report = script.validate(dataset("result").mode(DataSetValidator.ValidationMode.BySelector).size(4)
            .add(tuple().select("1234").field(string("Garbage")))
            .add(tuple().select("Battlestar").field(string("Galactica")))
            .add(tuple().select("Starship").field(string("Enterprise")))
            .add(tuple().select("12345").field(string("Collector")))
        );

        // -- print the test report
        System.out.println(StringReporter.format(report));

        Assert.assertTrue(report.isValid());
    }
}
