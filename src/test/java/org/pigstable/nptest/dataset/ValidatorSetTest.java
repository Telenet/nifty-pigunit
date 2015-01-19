package org.pigstable.nptest.dataset;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.pigstable.nptest.validator.FieldValidator.string;
import static org.pigstable.nptest.validator.TupleValidator.tuple;



public class ValidatorSetTest {

    @Test
    public void testAddValidatorToSet() throws Exception {
        ValidatorSet validatorSet = new ValidatorSet();
        validatorSet.add(tuple().field(string("SOHO")).field(string("SOHO")));

        assertThat(validatorSet.size(), is(equalTo(1)));
    }

    @Test
    public void testValidatorSetIsIterable() throws Exception {
        ValidatorSet validatorSet = new ValidatorSet();
        validatorSet.add(tuple().field(string("SOHO")));

        assertThat(validatorSet.iterator().hasNext(), is(true));
    }
}