package LSHFIndex;

import global.*;
import heap.*;
import iterator.CondExpr;
import iterator.Iterator;
import iterator.TupleIterator;
import iterator.FldSpec;
import java.io.*;
import java.util.*;

public class RSIndexScan extends Iterator {
    private LSHFIndex index;
    private Heapfile heapfile;
    private TupleIterator tupleIterator;

    // Constructor with the required signature
    public RSIndexScan(IndexType indexType, 
                       String relName, 
                       String indName, 
                       AttrType[] types, 
                       short[] str_sizes, 
                       int noInFlds, 
                       int noOutFlds, 
                       FldSpec[] outFlds, 
                       CondExpr[] selects, 
                       int fldNum, 
                       Vector100Dtype query, 
                       int distance) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        try {
            Vector100DKey queryKey = new Vector100DKey(query);
            // Load the LSHFIndex from disk
            String indexFilePath = indName.endsWith(".ser") ? indName : indName + ".ser";
            index = LSHFIndex.loadIndex(indexFilePath);

            // Open the heap file for the relation
            heapfile = new Heapfile(relName);

            // Perform the range search
            List<RID> rids = index.rangeSearch(queryKey, distance);

            // Fetch tuples corresponding to the RIDs
            List<Tuple> tuples = new ArrayList<>();
            for (RID rid : rids) {
                try {
                    Tuple tuple = heapfile.getRecord(rid);
                    tuple.setHdr((short) types.length, types, str_sizes);
                    tuples.add(tuple);
                } catch (InvalidSlotNumberException | InvalidTupleSizeException e) {
                    throw e; // Re-throw specific exceptions
                } catch (Exception e) {
                    throw new IOException("Error fetching record from heapfile", e);
                }
            }

            // Initialize an iterator over the fetched tuples
            tupleIterator = new TupleIterator(tuples);

        } catch (Exception e) {
            throw new IOException("Error initializing RSIndexScan", e);
        }
    }

    @Override
    public Tuple get_next() throws IOException {
        return tupleIterator.get_next();
    }

    @Override
    public void close() throws IOException {
        if (tupleIterator != null) {
            tupleIterator.close();
        }
    }
}