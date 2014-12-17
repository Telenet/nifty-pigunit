package org.pigstable.nptest.dataset;


import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FileDataSet {

    public static TestDataSet readTestData(String fileLocation, String delimiter) throws IOException {
        TestDataSet testdata = new TestDataSet();
        String s = FileUtils.readFileToString(new File(fileLocation));
        List<String> splits = FileUtils.readLines(new File(fileLocation));

        for(String record : splits)
        {
            testdata.add(record);
        }

        return testdata;
    }
}
