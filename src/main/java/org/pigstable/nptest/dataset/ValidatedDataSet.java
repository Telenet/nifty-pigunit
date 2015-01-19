package org.pigstable.nptest.dataset;

import com.google.common.collect.Lists;

import java.util.List;

import static org.pigstable.nptest.validator.TupleValidator.Builder;

public class ValidatedDataSet {

    List<Builder> tupleValidators = Lists.newArrayList();

    public void add(Builder tuple) {
        tupleValidators.add(tuple);
    }

    public List<Builder> getTuples() {
        return tupleValidators;
    }
}
