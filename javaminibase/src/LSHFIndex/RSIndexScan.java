package LSHFIndex;

import global.*;
import heap.*;
import iterator.CondExpr;
import iterator.Iterator;
import iterator.TupleIterator;
import iterator.FldSpec;
import java.io.*;
import java.util.*;

/**
 * RSIndexScan scans the LSHFIndex for records within a specified distance.
 * It retrieves candidate RIDs from the index and then fetches the corresponding tuples
 * from the heap file, ensuring uniqueness.
 */
public class RSIndexScan extends Iterator {
    private LSHFIndex index;
    private Heapfile heapfile;
    private TupleIterator tupleIterator;
    private AttrType[] types; // Store types for setting header
    private short[] str_sizes; // Store str_sizes for setting header

    /**
     * Constructor for RSIndexScan.
     *
     * @param indexType     The type of index (must be LSHFIndex).
     * @param relName       The name of the relation (heap file).
     * @param indName       The name of the LSH index file.
     * @param types         Array of attribute types for the relation.
     * @param str_sizes     Array of string sizes for the relation.
     * @param noInFlds      Number of input fields (unused, but part of signature).
     * @param noOutFlds     Number of output fields (unused, but part of signature).
     * @param outFlds       Projection specification (unused, projection happens later).
     * @param selects       Selection conditions (unused, filtering is by distance).
     * @param fldNum        The field number of the vector attribute.
     * @param query         The query vector.
     * @param distance      The maximum distance for the range query.
     * @throws IOException              If there is an I/O error.
     * @throws HFException              If there is a heap file error.
     * @throws HFBufMgrException        If there is a buffer manager error.
     * @throws HFDiskMgrException       If there is a disk manager error.
     * @throws InvalidSlotNumberException If an invalid slot number is encountered.
     * @throws InvalidTupleSizeException If an invalid tuple size is encountered.
     * @throws Exception                For other general errors during initialization.
     */
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
                       int distance) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, Exception {

        if (indexType.indexType != IndexType.LSHFIndex) {
            throw new IllegalArgumentException("RSIndexScan only supports LSHFIndex type.");
        }

        this.types = types; // Store for later use in setHdr
        this.str_sizes = str_sizes; // Store for later use in setHdr

        try {
            Vector100DKey queryKey = new Vector100DKey(query);
            // Load the LSHFIndex from disk
            String indexFilePath = indName.endsWith(".ser") ? indName : indName + ".ser";
            index = LSHFIndex.loadIndex(indexFilePath);
            System.out.println("DEBUG: Loaded LSH index from: " + indexFilePath);


            // Open the heap file for the relation
            heapfile = new Heapfile(relName);
            System.out.println("DEBUG: Opened heapfile: " + relName);


            // Perform the range search - This might return duplicate RIDs
            System.out.println("DEBUG: Performing LSH range search...");
            List<RID> rids = index.rangeSearch(queryKey, distance);
            System.out.println("DEBUG: LSH range search returned " + rids.size() + " candidate RIDs (may include duplicates).");


            // Fetch tuples corresponding to the UNIQUE RIDs
            List<Tuple> tuples = new ArrayList<>();
            Set<RID> processedRIDs = new HashSet<>(); // Keep track of processed RIDs to ensure uniqueness

            System.out.println("DEBUG: Fetching unique tuples from heapfile...");
            for (RID rid : rids) {
                // Only process if the RID hasn't been processed yet
                if (processedRIDs.add(rid)) {
                    try {
                        //System.out.println("DEBUG: Fetching record for unique RID: " + rid);
                        Tuple tuple = heapfile.getRecord(rid);
                        if (tuple != null) {
                            // IMPORTANT: Set the header on the tuple after fetching
                            tuple.setHdr((short) types.length, types, str_sizes);
                            tuples.add(tuple);
                            //System.out.println("DEBUG: Added tuple for RID: " + rid);
                        } else {
                             System.err.println("Warning: getRecord returned null for RID: " + rid);
                        }
                    } catch (InvalidSlotNumberException | InvalidTupleSizeException e) {
                        System.err.println("Error fetching record for RID " + rid + ": " + e.getMessage());
                        throw e; // Re-throw specific exceptions
                    } catch (Exception e) {
                         System.err.println("General error fetching record for RID " + rid + ": " + e.getMessage());
                        // Wrap other exceptions in IOException or a more specific custom exception if needed
                        throw new IOException("Error fetching record from heapfile for RID: " + rid, e);
                    }
                } else {
                     //System.out.println("DEBUG: Skipping duplicate RID: " + rid);
                }
            }
             System.out.println("DEBUG: Fetched " + tuples.size() + " unique tuples.");


            // Initialize an iterator over the fetched unique tuples
            tupleIterator = new TupleIterator(tuples);

        } catch (Exception e) {
            // Clean up resources if initialization fails partially
            if (heapfile != null) {
                try {
                    // Assuming Heapfile has a method to release resources without deleting
                    // If not, this might be tricky. For now, we rely on GC or higher-level cleanup.
                } catch (Exception cleanupEx) {
                    System.err.println("Error during cleanup after RSIndexScan init failure: " + cleanupEx.getMessage());
                }
            }
            // Log the root cause
             System.err.println("Error initializing RSIndexScan: " + e.getMessage());
             e.printStackTrace(); // Print stack trace for detailed debugging
            // Wrap and rethrow
            throw new IOException("Error initializing RSIndexScan", e);
        }
    }

    /**
     * Returns the next tuple in the scan.
     *
     * @return The next tuple, or null if the scan is complete.
     * @throws IOException If there is an error during iteration.
     */
    @Override
    public Tuple get_next() throws IOException {
        try {
            return tupleIterator.get_next();
        } catch (Exception e) {
            throw new IOException("Error getting next tuple from RSIndexScan", e);
        }
    }

    /**
     * Closes the iterator, releasing any resources.
     *
     * @throws IOException If there is an error closing the iterator.
     */
    @Override
    public void close() throws IOException {
        if (tupleIterator != null) {
            try {
                tupleIterator.close();
            } catch (Exception e) {
                throw new IOException("Error closing inner TupleIterator in RSIndexScan", e);
            }
        }
        // Note: We don't explicitly close the heapfile here as it might be shared
        // or managed by a higher-level component (like the SystemDefs buffer manager).
        // Closing it here could cause issues if other scans are using it.
        // The LSHFIndex object loaded from disk also doesn't typically require explicit closing unless it holds file handles.
    }
}