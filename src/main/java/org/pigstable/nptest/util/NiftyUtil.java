package org.pigstable.nptest.util;

import com.google.common.collect.Lists;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class NiftyUtil {

    private NiftyUtil()
    {
        //ignore
    }

    public static String[] getResult(Iterator<Tuple> result)
    {
        List<String> data = Lists.newArrayList();

        while(result.hasNext())
        {
            Tuple next = result.next();

            for(int i=0 ; i < next.size(); i++)
            {
                data.add(next.toString());
            }
        }

        return (String[])data.toArray();
    }

    public static boolean validate(String[] expected, String[] actual)
    {
        return Arrays.equals(expected, actual);
    }
}
