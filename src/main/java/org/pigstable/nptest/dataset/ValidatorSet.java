package org.pigstable.nptest.dataset;

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.Sets;

import java.util.Set;

import static org.pigstable.nptest.validator.TupleValidator.Builder;

public class ValidatorSet extends ForwardingSet<Builder> {

    private final Set<Builder> delegate = Sets.newHashSet();

    @Override
    protected Set<Builder> delegate() {
        return delegate;
    }
}
