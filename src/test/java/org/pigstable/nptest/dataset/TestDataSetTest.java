package org.pigstable.nptest.dataset;

import org.junit.Before;
import org.junit.Test;

public class TestDataSetTest {

    private TestDataSet testDataSet;

    @Before
    public void setUp()
    {
        testDataSet = new TestDataSet();
        testDataSet.add("asd");
        testDataSet.add("asd2");
    }

    @Test
    public void testGetDataSet() throws Exception {

        for(String data : testDataSet.getDataSet())
        {
            System.out.println(data);
        }

    }
}