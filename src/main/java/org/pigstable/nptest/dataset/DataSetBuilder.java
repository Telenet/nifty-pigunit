package org.pigstable.nptest.dataset;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;


/**
 * Builder to create a test data set.
 *
 * @author mlavaert
 */
public class DataSetBuilder extends ForwardingList<String> {

    private final List<String> delegate = Lists.newArrayList();

    /**
     * Private constructor, the object should be created using the factory methods.
     *
     * @param tuples The tuples that should be in the builder.
     */
    private DataSetBuilder(String... tuples) {
        delegate().addAll(Arrays.asList(tuples));
    }

    /**
     * Create a new empty data set builder.
     *
     * @return A new instance of {@link org.pigstable.nptest.dataset.DataSetBuilder}
     */
    public static DataSetBuilder empty() {
        return new DataSetBuilder();
    }

    /**
     * Create a new instance of {@link org.pigstable.nptest.dataset.DataSetBuilder} containing the given tuples.
     *
     * @param tuples The tuples that should be in the builder.
     * @return A new instance of {@link org.pigstable.nptest.dataset.DataSetBuilder}
     */
    public static DataSetBuilder of(String... tuples) {
        return new DataSetBuilder(tuples);
    }

    /**
     * Convert the data set to an array of String's
     *
     * @return The final data set.
     */
    public String[] build() {
        return delegate().toArray(new String[delegate().size()]);
    }

    @Override
    protected List<String> delegate() {
        return delegate;
    }
}