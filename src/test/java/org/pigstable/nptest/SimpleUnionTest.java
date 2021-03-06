package org.pigstable.nptest;

import com.google.common.collect.Lists;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.pigstable.nptest.dataset.TestDataSet;
import org.pigstable.nptest.dataset.ValidatedDataSet;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.validator.DataSetValidator;

import java.util.Iterator;
import java.util.List;

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

        TestDataSet setA = new TestDataSet();

        setA.add("1393801;Supplier1");
        setA.add("139380;Supplier2");

        TestDataSet setB = new TestDataSet();

        setB.add("SOHO;Supplier3");
        setB.add("9xaiqa00840tx05pp0kqi;Supplier4");

        test.input("setA", setA, NiftyPigTest.STORAGE_PIG_CSV);
        test.input("setB", setB, NiftyPigTest.STORAGE_PIG_CSV);

        // -- actually execute the pig script

        test.execute();

        ValidatedDataSet validatedDataset = new ValidatedDataSet();


        validatedDataset.add(tuple().field(string("SOHO")).field(string("Supplier3")));
        validatedDataset.add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("Supplier4")));
        validatedDataset.add(tuple().field(string("1393801")).field(string("Supplier1")));
        validatedDataset.add(tuple().field(string("139380")).field(string("Supplier2")));

        //Validate with new api
        DataSetReport report = test.validate("result", validatedDataset, DataSetValidator.ValidationMode.ByOrder,4);

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }

}
