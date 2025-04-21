package iterator;

import global.*;
import heap.*;
import iterator.Iterator;
import java.io.*;
import java.util.*;

public class TupleIterator extends Iterator {
    private List<Tuple> tupleList; // List of tuples to iterate over
    private int currentIndex;     // Current index in the list

    public TupleIterator(List<Tuple> tuples) {
        this.tupleList = tuples;
        this.currentIndex = 0; // Start at the beginning of the list
    }

    @Override
    public Tuple get_next() throws IOException {
        if (currentIndex < tupleList.size()) {
            return tupleList.get(currentIndex++);
        }
        return null; // No more tuples
    }

    @Override
    public void close() throws IOException {
        // Clear the list to release memory
        tupleList.clear();
    }
}