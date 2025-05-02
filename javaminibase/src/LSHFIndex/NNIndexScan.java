package LSHFIndex;

import global.*;
import heap.*;
import iterator.*;
import java.io.*;
import java.util.*;
import iterator.Iterator; // Explicit import for clarity

/**
 * NNIndexScan retrieves tuples in nearest neighbor order using an LSHFIndex.
 * It fetches the top K candidates from the index and then retrieves the
 * corresponding tuples from the heap file one by one.
 */
public class NNIndexScan extends Iterator {
    private LSHFIndex index; // The LSH index structure
    private Heapfile heapfile; // The heap file containing the actual tuples
    private List<RIDDistancePair> sortedPairs; // Stores the sorted pairs (RID, distance) from LSHFIndex
    private int currentIndex; // Tracks the current position in the sortedPairs list
    private double lastDistance; // Stores the distance of the last tuple returned by get_next()
    private AttrType[] schemaTypes; // Stores the full schema types for setting tuple headers
    private short[] schemaStrSizes; // Stores the full schema string sizes for setting tuple headers

    /**
     * Constructor for NNIndexScan.
     *
     * @param indexType The type of index (should be LSHFIndex).
     * @param relName The name of the relation (heap file).
     * @param indName The name of the index file (.ser file).
     * @param types The attribute types of the relation schema.
     * @param str_sizes The string sizes for string attributes in the relation schema.
     * @param noInFlds Number of fields in the input tuple (relation schema).
     * @param noOutFlds Number of fields in the output tuple (projection).
     * @param outFlds Projection specification.
     * @param selects Selection conditions (not used by NNIndexScan itself).
     * @param fldNum The field number (1-based) of the vector attribute being queried.
     * @param query The target vector for the NN search.
     * @param count The number of nearest neighbors (K) to retrieve.
     * @throws IOException If an I/O error occurs.
     * @throws HFException Heap file exception.
     * @throws HFBufMgrException Buffer manager exception.
     * @throws HFDiskMgrException Disk manager exception.
     * @throws InvalidSlotNumberException Invalid slot number during tuple retrieval.
     * @throws InvalidTupleSizeException Invalid tuple size during tuple retrieval.
     * @throws ClassNotFoundException If the LSHFIndex class definition is not found during deserialization.
     */
    public NNIndexScan(IndexType indexType,
                       String relName,
                       String indName,
                       AttrType[] types,
                       short[] str_sizes,
                       int noInFlds, // Not directly used here, but part of signature
                       int noOutFlds, // Not directly used here, but part of signature
                       FldSpec[] outFlds, // Not directly used here, but part of signature
                       CondExpr[] selects, // Not directly used here, but part of signature
                       int fldNum, // Not directly used here, but part of signature
                       Vector100Dtype query,
                       int count) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException, ClassNotFoundException {

        // Store schema information needed for setting tuple headers later
        this.schemaTypes = types;
        this.schemaStrSizes = str_sizes;

        try {
            // Prepare the query key
            Vector100DKey queryKey = new Vector100DKey(query);

            // Load the LSHFIndex from the specified file
            String indexFilePath = indName.endsWith(".ser") ? indName : indName + ".ser";
            index = LSHFIndex.loadIndex(indexFilePath);

            // Open the heap file containing the relation data
            heapfile = new Heapfile(relName);

            // Perform the nearest neighbor search using the LSH index.
            // This method is expected to return a list sorted by distance.
            sortedPairs = index.nearestNeighborSearch(queryKey, count);

            // Initialize the index for iterating through the sorted results
            currentIndex = 0;
            lastDistance = -1.0; // Initialize distance to an invalid value

        } catch (ClassNotFoundException e) {
             // Specific handling for class not found during deserialization
             throw new IOException("Error initializing NNIndexScan: Could not find class definition for index file.", e);
        } catch (Exception e) {
            // Catch other potential exceptions (IO, Heapfile, LSHFIndex internal errors)
            throw new IOException("Error initializing NNIndexScan", e);
        }
    }

    /**
     * Retrieves the next tuple in the nearest neighbor order.
     * Fetches the tuple corresponding to the next RID in the sorted list
     * obtained from the LSH index.
     *
     * @return The next Tuple in NN order, or null if no more tuples exist.
     * @throws IOException If an error occurs during heap file access.
     * @throws InvalidTupleSizeException If a tuple size mismatch occurs.
     * @throws InvalidSlotNumberException If an invalid slot is encountered.
     */
    @Override
    public Tuple get_next() throws IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        // Check if we have processed all pairs returned by the index search
        if (currentIndex >= sortedPairs.size()) {
            lastDistance = -1.0; // Reset distance when iteration is complete
            return null; // No more tuples
        }

        // Get the next pair (RID and its associated distance) from the sorted list
        RIDDistancePair pair = sortedPairs.get(currentIndex);
        Tuple tuple = null;

        try {
            // Fetch the actual tuple data from the heap file using the RID
            tuple = heapfile.getRecord(pair.rid);

            if (tuple != null) {
                // Set the header on the retrieved tuple using the stored schema info
                // This is necessary for subsequent operations (like projection) to interpret the tuple correctly.
                tuple.setHdr((short) schemaTypes.length, schemaTypes, schemaStrSizes);

                // Store the distance associated with this tuple, so it can be retrieved later
                lastDistance = pair.distance;

                // Advance the index to the next position for the subsequent call
                currentIndex++;
            } else {
                 // This case should ideally not happen if the index contains valid RIDs,
                 // but handle it defensively. Log a warning and try to skip to the next.
                 System.err.println("Warning: NNIndexScan could not fetch record for RID: " + pair.rid + ". Skipping.");
                 lastDistance = -1.0; // Reset distance for the skipped entry
                 currentIndex++; // Move past the problematic RID
                 return get_next(); // Recursively try to fetch the next valid tuple
            }
        } catch (InvalidSlotNumberException | InvalidTupleSizeException e) {
            // Handle specific heap file exceptions during record fetching
            System.err.println("Error fetching record in NNIndexScan.get_next() for RID " + pair.rid + ": " + e.getMessage());
            lastDistance = -1.0;
            currentIndex++; // Attempt to skip the problematic RID
            throw e; // Re-throw the exception
        } catch (Exception e) {
            // Catch any other unexpected exceptions during record fetching
            System.err.println("Generic error fetching record in NNIndexScan.get_next() for RID " + pair.rid + ": " + e.getMessage());
            lastDistance = -1.0;
            currentIndex++; // Attempt to skip
            throw new IOException("Error fetching record from heapfile in NNIndexScan", e);
        }

        // Return the successfully fetched and prepared tuple
        return tuple;
    }

    /**
     * Gets the distance associated with the last tuple that was successfully
     * returned by the {@link #get_next()} method.
     *
     * @return The distance calculated by the LSH index search for the last tuple,
     *         or -1.0 if {@link #get_next()} hasn't been called successfully yet
     *         or if the iteration has finished.
     */
    public double get_last_distance() {
        return lastDistance;
    }


    /**
     * Closes the iterator. Releases any resources held.
     * In this implementation, it primarily resets the internal state.
     * The underlying Heapfile is typically managed globally and not closed here.
     *
     * @throws IOException If an error occurs during closing (unlikely here).
     */
    @Override
    public void close() throws IOException {
        // Reset internal state to allow garbage collection and prevent reuse
        currentIndex = 0;
        sortedPairs = null; // Clear the list of pairs
        lastDistance = -1.0;
        // Note: We don't close the heapfile here as it's likely shared/managed elsewhere.
    }
}