package org.pigstable.nptest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.pigstable.nptest.dataset.MappedDataSet;
import org.pigstable.nptest.dataset.ValidateMappedDataSet;
import org.pigstable.nptest.reporter.StringReporter;
import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.validator.DataSetValidator;
import org.pigstable.nptest.validator.FieldValidator;

import java.util.List;
import java.util.Map;

import static org.pigstable.nptest.validator.FieldValidator.string;

public class MappedDataTest {

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
        MappedDataSet setA = new MappedDataSet(mappings);

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
        MappedDataSet setB = new MappedDataSet(mappings);

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

        ValidateMappedDataSet validdata = new ValidateMappedDataSet(mappings);

        Map<String,FieldValidator> testValidator1 = Maps.newHashMap();

        testValidator1.put("col1",string("139380"));
        testValidator1.put("col2",string("AD210"));

        Map<String,FieldValidator> testValidator2 = Maps.newHashMap();

        testValidator2.put("col1",string("139380"));
        testValidator2.put("col2",string("AD2100"));

        Map<String,FieldValidator> testValidator3 = Maps.newHashMap();

        testValidator3.put("col1",string("SOHO"));
        testValidator3.put("col2",string("SOHO"));

        Map<String,FieldValidator> testValidator4 = Maps.newHashMap();

        testValidator4.put("col1",string("9xaiqa00840tx05pp0kqi"));
        testValidator4.put("col2",string("SOHO"));

        validdata.add(testValidator1);
        validdata.add(testValidator2);
        validdata.add(testValidator3);
        validdata.add(testValidator4);


        //Validate with new api
        DataSetReport report = test.validate("result", validdata, DataSetValidator.ValidationMode.ByOrder,4);

        // -- print the test report
        System.out.println(StringReporter.format(report));

        // -- check the report was valid
        Assert.assertTrue(report.isValid());
    }
}
