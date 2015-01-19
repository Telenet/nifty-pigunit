package org.pigstable.nptest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.pigstable.nptest.dataset.DataSetBuilder;
import org.pigstable.nptest.dataset.ValidatorSet;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.test.ClassPathResource;
import org.pigstable.nptest.validator.DataSetValidator;

import static org.pigstable.nptest.validator.FieldValidator.string;
import static org.pigstable.nptest.validator.TupleValidator.tuple;

public class SimpleUnionTest {

    private NiftyPigTest test;

    @Before
    public void setUp() throws Exception {
        this.test = new NiftyPigTest(ClassPathResource.create("simpleUnion.pig").systemPath());
    }

    @Test
    @Category(TestCategories.PigTest.class)
    public void testTextInput() throws Exception{
        DataSetBuilder setA = DataSetBuilder.of(
                "139380;AD210",
                "139380;AD2100"
        );
        test.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);

        DataSetBuilder setB = DataSetBuilder.of(
                "SOHO;SOHO",
                "9xaiqa00840tx05pp0kqi;SOHO"
        );

        test.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script
        test.execute();

        ValidatorSet validatorSet = new ValidatorSet();

        validatorSet.add(tuple().field(string("SOHO")).field(string("SOHO")));
        validatorSet.add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")));
        validatorSet.add(tuple().field(string("139380")).field(string("AD210")));
        validatorSet.add(tuple().field(string("139380")).field(string("AD2100")));

        //Validate with new api
        DataSetReport report = test.validate("result", validatorSet, DataSetValidator.ValidationMode.ByOrder,4);

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }
}