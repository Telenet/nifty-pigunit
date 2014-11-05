package org.pigstable.nptest.dataset;

import com.google.common.collect.Lists;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.Arrays;
import java.util.List;

public class TestDataSet {
    private List<String> tuples = Lists.newArrayList();
    private Schema schema;

    public TestDataSet(Schema schema)
    {
        this.schema = schema;
    }

    public void add(String data)
    {
        tuples.add(data);
    }

    public void addAll(List<String> data)
    {
        tuples.addAll(data);
    }

    public String[] getDataSet()
    {
        Object[] objects = tuples.toArray();

        String[] data = new String[objects.length];

        for(int i=0; i < objects.length; i ++)
        {
            data[i] = (String)objects[i];
        }

        return data;
    }
}
