package org.pigstable.nptest.dataset;


import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class MappedDataSet {

    private List<String> schema;
    private List<Map<String, String>> tuples = Lists.newArrayList();

    public MappedDataSet(List<String> schema)
    {
        this.schema = schema;
    }

    public void add(Map<String, String> tuple) {
        tuples.add(tuple);
    }

    public List<String> getSchema() {
        return schema;
    }

    public List<Map<String, String>> getTuples()
    {
        return tuples;
    }
}
