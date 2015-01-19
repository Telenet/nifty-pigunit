package org.pigstable.nptest.dataset;


import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class MappedDataset extends ForwardingList<Map<String, String>> {

    private final List<Map<String, String>> delegate = Lists.newArrayList();
    private final List<String> schema;

    public MappedDataset(List<String> schema) {
        this.schema = schema;
    }

    public List<String> getSchema() {
        return schema;
    }

    @Override
    protected List<Map<String, String>> delegate() {
        return delegate;
    }
}