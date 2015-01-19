package org.pigstable.nptest.dataset;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.ForwardingSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static org.pigstable.nptest.validator.TupleValidator.Builder;

/**
 * Custom collection to store validators.
 */
public final class ValidatorSet extends ForwardingList<Builder> {

    private final List<Builder> delegate = Lists.newArrayList();

    @Override
    protected List<Builder> delegate() {
        return delegate;
    }
}