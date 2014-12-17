package org.pigstable.nptest.dataset;

import com.google.common.collect.Lists;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.Arrays;
import java.util.List;

public class TestDataSet{
    private List<String> tuples = Lists.newArrayList();
    private Schema schema;

    public TestDataSet(Schema schema)
    {
        this.schema = schema;
    }

    public TestDataSet()
    {

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
        String[] data = new String[tuples.size()];

        for(int i=0; i < tuples.size(); i ++)
        {
            data[i] = tuples.get(i);
        }

        return data;
    }
}
