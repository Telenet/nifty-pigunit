package org.pigstable.nptest;

import com.google.common.base.Objects;

public final class FieldObject<T> implements Comparable<FieldObject> {

    private final int fieldSequence;
    private final T object;

    public FieldObject(int fieldSequence, T object) {
        this.fieldSequence = fieldSequence;
        this.object = object;
    }

    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.fieldSequence == ((FieldObject) other).fieldSequence;
    }

    private boolean canEqual(Object other) {
        return (this == other) || !(other == null || getClass() != other.getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldSequence);
    }

    public final int getFieldSequence() {
        return fieldSequence;
    }

    public final T getObject() {
        return object;
    }

    @Override
    public int compareTo(FieldObject o) {
        return o.fieldSequence - fieldSequence;
    }
}