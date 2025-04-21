package global;

import java.io.Serializable;
import java.util.Arrays;

public class Vector100Dtype implements Serializable {
    private static final int DIMENSIONS = 100;
    private int[] values;

    /**
     * Constructor to initialize a 100-dimensional vector.
     * 
     * @param values An array of 100 integers representing the vector.
     * @throws IllegalArgumentException if the array is not of size 100 or contains invalid values.
     */
    public Vector100Dtype(int[] values) {
        if (values.length != DIMENSIONS) {
            throw new IllegalArgumentException("Vector must have exactly 100 dimensions.");
        }
        for (int value : values) {
            if (value < -10000 || value > 10000) {
                throw new IllegalArgumentException("Vector values must be in the range [-10000, 10000].");
            }
        }
        this.values = Arrays.copyOf(values, DIMENSIONS);
    }

    /**
     * Default constructor to initialize a zero vector.
     */
    public Vector100Dtype() {
        this.values = new int[DIMENSIONS];
    }

    /**
     * Get the value at a specific dimension.
     * 
     * @param index The dimension index (0-based).
     * @return The value at the specified dimension.
     * @throws IndexOutOfBoundsException if the index is out of range.
     */
    public int getValue(int index) {
        if (index < 0 || index >= DIMENSIONS) {
            throw new IndexOutOfBoundsException("Index must be between 0 and 99.");
        }
        return values[index];
    }

    /**
     * Set the value at a specific dimension.
     * 
     * @param index The dimension index (0-based).
     * @param value The value to set (must be in the range [-10000, 10000]).
     * @throws IndexOutOfBoundsException if the index is out of range.
     * @throws IllegalArgumentException if the value is out of range.
     */
    public void setValue(int index, int value) {
        if (index < 0 || index >= DIMENSIONS) {
            throw new IndexOutOfBoundsException("Index must be between 0 and 99.");
        }
        if (value < -10000 || value > 10000) {
            throw new IllegalArgumentException("Value must be in the range [-10000, 10000].");
        }
        values[index] = value;
    }

    /**
     * Get the entire vector as an array.
     * 
     * @return A copy of the vector values.
     */
    public int[] getValues() {
        return Arrays.copyOf(values, DIMENSIONS);
    }

    /**
     * Calculate the Euclidean distance between this vector and another vector.
     * 
     * @param other The other vector.
     * @return The Euclidean distance.
     * @throws IllegalArgumentException if the other vector is null.
     */
    public double distanceTo(Vector100Dtype other) {
        if (other == null) {
            throw new IllegalArgumentException("Other vector cannot be null.");
        }
        double sum = 0;
        for (int i = 0; i < DIMENSIONS; i++) {
            int diff = this.values[i] - other.values[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Vector100Dtype other = (Vector100Dtype) obj;
        return Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }
}