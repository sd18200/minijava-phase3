package global;

import iterator.TupleUtils;
import java.io.*;
import btree.*;

public class Vector100DKey extends KeyClass implements Serializable {
    private Vector100Dtype vector;
    
    public Vector100DKey(Vector100Dtype vector) {
        if (vector == null) {
            throw new IllegalArgumentException("Vector cannot be null");
        }
        this.vector = vector;
    }
    
    public Vector100Dtype getVector() {
        return new Vector100Dtype(vector.getValues());
    }

    public void setVector(Vector100Dtype vector) {
        this.vector = new Vector100Dtype(vector.getValues());
    }
    
    public int compareTo(KeyClass key) {
        if (!(key instanceof Vector100DKey))
            throw new ClassCastException("Key is not a Vector100DKey");
            
        Vector100Dtype otherVector = ((Vector100DKey)key).getVector();
        // Compare based on distance from origin
        double myDist = TupleUtils.calculateEuclideanDistance(vector.getValues(), new int[100]);
        double otherDist = TupleUtils.calculateEuclideanDistance(otherVector.getValues(), new int[100]);
        return Double.compare(myDist, otherDist);
    }
    
    public void writeToPage(byte[] page, int offset) throws IOException {
        Convert.setVector100DtypeValue(vector, offset, page);
    }
    
    public static Vector100DKey readFromPage(byte[] page, int offset) throws IOException {
        Vector100Dtype vector = Convert.getVector100DtypeValue(offset, page);
        return new Vector100DKey(vector);
    }
    
    @Override
    public String toString() {
        return vector.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Vector100DKey other = (Vector100DKey) obj;
        return vector.equals(other.vector);
    }
    
    @Override
    public int hashCode() {
        return vector.hashCode();
    }
}