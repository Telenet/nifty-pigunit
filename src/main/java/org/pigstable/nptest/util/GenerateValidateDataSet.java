package org.pigstable.nptest.util;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.pigstable.nptest.NiftyPigTest;
import org.pigstable.nptest.dataset.ValidatedDataSet;
import org.pigstable.nptest.validator.FieldValidator;
import org.pigstable.nptest.validator.TupleValidator;

import java.io.File;
import java.io.IOException;

public class GenerateValidateDataSet {

    private GenerateValidateDataSet()
    {
        //ignore
    }

    public ValidatedDataSet createValidateDataSet(File testFile, String delimiter)
    {
        ValidatedDataSet validatedDataSet = new ValidatedDataSet();

        try {

            String test_data = FileUtils.readFileToString(testFile);
            String[] rows = test_data.split("\\n");


            for(String row : rows)
            {
                TupleValidator.Builder tupleValidator = TupleValidator.tuple();

                String[] fields = row.split(delimiter);
                for(String field: fields)
                {
                    tupleValidator.field(FieldValidator.string(field));
                }

                validatedDataSet.add(tupleValidator);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return validatedDataSet;
    }

}
