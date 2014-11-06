package org.pigstable.nptest.dataset;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.pigstable.nptest.validator.FieldValidator;
import org.pigstable.nptest.validator.TupleValidator;

import java.util.List;
import java.util.Map;

public class ValidateMappedDataSet {
    private List<String> schema = Lists.newArrayList();
    private List<Map<String,FieldValidator>> testData = Lists.newArrayList();

    public ValidateMappedDataSet(List<String> schema) {

        this.schema = schema;
    }

    public void add(Map<String, FieldValidator> testValidator1) {

        testData.add(testValidator1);
    }

    public List<String> getSchema() {
        return schema;
    }

    public List<Map<String,FieldValidator>> getTuples() {
        return testData;
    }
}
