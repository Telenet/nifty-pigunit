package org.pigstable.nptest;

public class FieldObject<T> implements Comparable<FieldObject> {
    private int fieldSequence;
    private T object;

    public FieldObject(int fieldSequence, T object) {
        this.fieldSequence = fieldSequence;
        this.object = object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldObject that = (FieldObject) o;

        return fieldSequence == that.fieldSequence;

    }

    @Override
    public int hashCode() {
        return fieldSequence;
    }

    public int getFieldSequence() {
        return fieldSequence;
    }

    public T getObject() {
        return object;
    }

    @Override
    public int compareTo(FieldObject o) {
        return o.fieldSequence - fieldSequence;
    }
}
