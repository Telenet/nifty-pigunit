package org.pigstable.nptest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.pigstable.nptest.dataset.MappedDataset;
import org.pigstable.nptest.dataset.ValidatedDataSet;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.validator.DataSetValidator;

import java.util.List;
import java.util.Map;

import static org.pigstable.nptest.validator.DataSetValidator.dataset;
import static org.pigstable.nptest.validator.FieldValidator.string;
import static org.pigstable.nptest.validator.TupleValidator.tuple;

public class ValidateDataSetTest {
    private static final String PIG_SCRIPT = "simpleUnion.pig";

    @Test
    @Category(TestCategories.PigTest.class)
    public void testTextInput() throws Exception{
        // -- initialize the pig testing class
        NiftyPigTest test = new NiftyPigTest(PIG_SCRIPT);

        //define schema
        List<String> mappings = Lists.newArrayList();

        mappings.add("col1");
        mappings.add("col2");

        //Map schema to data
        MappedDataset setA = new MappedDataset(mappings);

        Map<String,String> data1 = Maps.newHashMap();
        data1.put("col1","139380");
        data1.put("col2","AD210");

        Map<String,String> data2 = Maps.newHashMap();
        data2.put("col1","139380");
        data2.put("col2","AD2100");

        setA.add(data1);
        setA.add(data2);

        test.input("setA", setA);

        //Map schema to data
        MappedDataset setB = new MappedDataset(mappings);

        Map<String,String> data3 = Maps.newHashMap();

        data3.put("col1","SOHO");
        data3.put("col2","SOHO");

        Map<String,String> data4 = Maps.newHashMap();

        data4.put("col1","9xaiqa00840tx05pp0kqi");
        data4.put("col2","SOHO");

        setB.add(data3);
        setB.add(data4);

        test.input("setB", setB);

        // -- actually execute the pig script
        test.execute();

        ValidatedDataSet validatedDataset = new ValidatedDataSet();

        validatedDataset.add(tuple().field(string("SOHO")).field(string("SOHO")));
        validatedDataset.add(tuple().field(string("9xaiqa00840tx05pp0kqi")).field(string("SOHO")));
        validatedDataset.add(tuple().field(string("139380")).field(string("AD210")));
        validatedDataset.add(tuple().field(string("139380")).field(string("AD2100")));

        //Validate with new api
        DataSetReport report = test.validate("result", validatedDataset, DataSetValidator.ValidationMode.ByOrder,4);

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }

    @Test
    @Category(TestCategories.PigTest.class)
    public void testValidationBySelectors() throws Exception {

        NiftyPigTest script = new NiftyPigTest(PIG_SCRIPT);

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

        ValidatedDataSet validatedDataSet = new ValidatedDataSet();
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
