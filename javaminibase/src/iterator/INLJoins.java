package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import index.*;
import java.io.*;
import java.util.List;

import btree.*; // For BTreeFile and BTFileScan
import LSHFIndex.*; // For LSHFIndex and LSHFFileRangeScan

/**
 * This file contains an implementation of the Index Nested Loop Join
 * algorithm as specified in Phase 3, Task 1.
 */
public class INLJoins extends Iterator {
    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer;
    private short t1_str_sizes[], t2_str_sizes[];
    private CondExpr OutputFilter[];
    private CondExpr RightFilter[]; // Note: Typically less useful in INLJ, index scan is the main filter
    private int n_buf_pgs; // # of buffer pages available.
    private boolean done, // Is the join complete
            get_from_outer; // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple; // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile innerHeapFile;
    private Object currentIndexScan; // Can be BTFileScan or ILSHFileScan 
    private String innerRelName;
    private IndexType innerIndexType;
    private String innerIndexName;

    private int joinCol1, joinCol2; // Join column field numbers
    private int vectorJoinDistance; // Distance for vector joins
    
    // Resource management fields
    private BTreeFile currentBTreeFile;  // Persistent BTreeFile reference
    private LSHFIndex currentLSHIndex;   // Persistent LSHIndex reference

    /**
     * Constructor
     * Initialize the join operator
     * 
     * @param in1          Array containing field types of R.
     * @param len_in1      # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param in2          Array containing field types of S
     * @param len_in2      # of columns in S.
     * @param t2_str_sizes shows the length of the string fields.
     * @param amt_of_mem   IN PAGES
     * @param am1          access method for left i/p to join
     * @param relationName access method for right i/p to join
     * @param index        type of index to use (BTree/LSH)
     * @param indexName    name of the index file
     * @param outFilter    select expressions
     * @param rightFilter  reference to filter applied on right i/p
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fields
     * @throws IOException         some I/O fault
     * @throws NestedLoopException exception from this class
     */
    public INLJoins(AttrType in1[], int len_in1, short t1_str_sizes[],
            AttrType in2[], int len_in2, short t2_str_sizes[],
            int amt_of_mem,
            Iterator am1,
            String relationName, IndexType index, String indexName,
            CondExpr outFilter[],
            CondExpr rightFilter[], // Often null or less critical for INLJ
            FldSpec proj_list[], int n_out_flds)
            throws IOException, NestedLoopException {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        this.t1_str_sizes = t1_str_sizes; 
        this.t2_str_sizes = t2_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter = rightFilter; // Store it, though its use might be limited

        n_buf_pgs = amt_of_mem;
        innerRelName = relationName;
        innerIndexType = index;
        innerIndexName = indexName;
        currentIndexScan = null; // Initialize scan to null
        
        // Initialize resource management fields
        currentBTreeFile = null;
        currentLSHIndex = null;
        
        done = false;
        get_from_outer = true;

        // Allocate space for the joined tuple
        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;
        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, n_out_flds);
        } catch (TupleUtilsException e) {
            throw new NestedLoopException(e, "TupleUtilsException is caught by INLJoins.java");
        }

        try {
            innerHeapFile = new Heapfile(innerRelName);
        } catch (Exception e) {
            throw new NestedLoopException(e, "Create new heapfile failed.");
        }

        // --- Identify Join Columns and Vector Distance ---
        // This is a simplified approach; a robust parser would handle complex filters.
        // Assumes the primary join condition is the first in OutputFilter.
        if (OutputFilter != null && OutputFilter.length > 0) {
            CondExpr joinExpr = OutputFilter[0]; // Assuming the first is the join predicate
            if (joinExpr.operand1.symbol != null && joinExpr.operand2.symbol != null) {
                if (joinExpr.operand1.symbol.relation.key == RelSpec.outer) {
                    joinCol1 = joinExpr.operand1.symbol.offset;
                    joinCol2 = joinExpr.operand2.symbol.offset;
                } else {
                    joinCol1 = joinExpr.operand2.symbol.offset;
                    joinCol2 = joinExpr.operand1.symbol.offset;
                }

                // Check if it's a vector join and get distance
                if (joinCol1 > 0 && joinCol2 > 0 && 
                    _in1[joinCol1 - 1].attrType == AttrType.attrVector100D &&
                    _in2[joinCol2 - 1].attrType == AttrType.attrVector100D) {
                    if (innerIndexType.indexType != IndexType.LSHFIndex) {
                        System.err.println("Warning: Vector join requested but inner index is not LSHFIndex.");
                        // Potentially throw an exception or proceed with caution
                    }
                    vectorJoinDistance = joinExpr.distance; // Get distance from CondExpr
                    if (vectorJoinDistance < 0) {
                        throw new NestedLoopException("Invalid negative distance for vector join.");
                    }
                } else if (innerIndexType.indexType == IndexType.LSHFIndex) {
                    System.err.println("Warning: LSHFIndex specified for non-vector join attribute.");
                    // Potentially throw an exception
                }
            } else {
                throw new NestedLoopException("Invalid join condition format in OutputFilter.");
            }
        } else {
            throw new NestedLoopException("Index Nested Loop Join requires a join condition in OutputFilter.");
        }
        
        // Pre-load indexes if possible to avoid reloading for each tuple
        try {
            if (innerIndexType.indexType == IndexType.B_Index) {
                currentBTreeFile = new BTreeFile(innerIndexName);
            } else if (innerIndexType.indexType == IndexType.LSHFIndex) {
                String indexFilePath = innerIndexName.endsWith(".ser") ? innerIndexName : innerIndexName + ".ser";
                try {
                    currentLSHIndex = LSHFIndex.loadIndex(indexFilePath);
                } catch (ClassNotFoundException e) {
                    throw new NestedLoopException(e, "Failed to load LSH index file");
                }
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not pre-load index: " + e.getMessage());
            // Continue anyway, we'll try loading on demand
        }
    }

    /**
     * @return The joined tuple is returned
     * @exception IOException               I/O errors
     * @exception JoinsException            some join exception
     * @exception IndexException            exception from super class
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception InvalidTypeException      tuple type not valid
     * @exception PageNotReadException      exception from lower layer
     * @exception TupleUtilsException       exception from using tuple utilities
     * @exception PredEvalException         exception from PredEval class
     * @exception SortException             sort exception
     * @exception LowMemException           memory error
     * @exception UnknowAttrType            attribute type unknown
     * @exception UnknownKeyTypeException   key type unknown
     * @exception Exception                 other exceptions
     */
    public Tuple get_next()
            throws IOException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception {
        if (done)
            return null;

        do {
            // If we need to get a new tuple from the outer relation.
            if (get_from_outer) {
                get_from_outer = false;
                // Close previous index scan if it exists
                closeCurrentScan();

                // Get the next tuple from the outer relation
                if ((outer_tuple = outer.get_next()) == null) {
                    done = true; // Outer relation exhausted
                    // Clean up any remaining resources before returning null
                    closeCurrentScan();
                    return null;
                }

                // Now, setup and execute the index scan on the inner relation
                try {
                    if (innerIndexType.indexType == IndexType.B_Index) {
                        // --- BTree Index Scan ---
                        KeyClass outerKey = extractKeyFromTuple(outer_tuple, joinCol1);
                        if (currentBTreeFile == null) {
                            // Create the BTree file if not already loaded
                            currentBTreeFile = new BTreeFile(innerIndexName);
                        }
                        currentIndexScan = currentBTreeFile.new_scan(outerKey, outerKey); // Scan for exact match
                    } else if (innerIndexType.indexType == IndexType.LSHFIndex) {
                        // --- LSH Index Scan ---
                        int[] vectorArray = outer_tuple.getVectorFld(joinCol1);
                        // Create a Vector100Dtype from the int array
                        Vector100Dtype outerVector = new Vector100Dtype(vectorArray);
                        
                        // Create a Vector100DKey from the Vector100Dtype
                        Vector100DKey outerKey = new Vector100DKey(outerVector);
                        
                        // Load the LSH index from disk if not already loaded
                        if (currentLSHIndex == null) {
                            String indexFilePath = innerIndexName.endsWith(".ser") ? innerIndexName : innerIndexName + ".ser";
                            try {
                                currentLSHIndex = LSHFIndex.loadIndex(indexFilePath);
                            } catch (ClassNotFoundException e) {
                                throw new JoinsException(e, "Failed to load LSH index file");
                            }
                        }
                        
                        // Get RIDs using rangeSearch
                        List<RID> ridList = currentLSHIndex.rangeSearch(outerKey, vectorJoinDistance);
                        
                        // Store the RID list and create an iterator for it
                        currentIndexScan = ridList.iterator();
                    } else {
                        throw new JoinsException("Unsupported index type for Index Nested Loop Join: " + innerIndexType);
                    }
                } catch (Exception e) {
                    // Clean up outer tuple fetch if index scan setup fails
                    outer_tuple = null;
                    get_from_outer = true;
                    throw new JoinsException(e, "Failed to initiate index scan on inner relation.");
                }
            }

            // --- Inner Loop: Iterate through index scan results ---
            RID inner_rid = getNextRIDFromScan();

            if (inner_rid == null) {
                // Inner scan exhausted for the current outer tuple.
                closeCurrentScan();
                get_from_outer = true; // Need to get the next outer tuple.
                continue; // Continue the outer loop
            }

            // RID found, fetch the inner tuple from the heap file.
            try {
                // Fix: Correctly handle getting a tuple from the heapfile
                Tuple temp = innerHeapFile.getRecord(inner_rid);
                inner_tuple.tupleCopy(temp);
                inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizes);
            } catch (Exception e) {
                System.err.println("Failed to retrieve inner tuple for RID: " + e.getMessage());
                continue; // Try next RID
            }

            // Apply RightFilter if present
            if (RightFilter != null && !PredEval.Eval(RightFilter, inner_tuple, null, _in2, null)) {
                continue; // Inner tuple fails RightFilter, get next
            }

            // Apply OutputFilter (the join condition + potentially others)
            if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2)) {
                // Qualification passed, project and return the joined tuple
                Projection.Join(outer_tuple, _in1,
                        inner_tuple, _in2,
                        Jtuple, perm_mat, nOutFlds);
                return Jtuple;
            }

        } while (true);
    }

    /**
     * Extract a key from the tuple based on the column number
     */
    private KeyClass extractKeyFromTuple(Tuple tuple, int columnNumber) 
        throws IOException, FieldNumberOutOfBoundException, UnknowAttrType {
        AttrType attrType = _in1[columnNumber - 1];
        switch (attrType.attrType) {
            case AttrType.attrInteger:
                return new IntegerKey(tuple.getIntFld(columnNumber));
            case AttrType.attrReal:
                // Convert float to string since there's no FloatKey class
                float floatValue = tuple.getFloFld(columnNumber);
                return new StringKey(Float.toString(floatValue));
            case AttrType.attrString:
                return new StringKey(tuple.getStrFld(columnNumber));
            case AttrType.attrVector100D:
                int[] vectorData = tuple.getVectorFld(columnNumber);
                Vector100Dtype vectorObj = new Vector100Dtype(vectorData);
                return new Vector100DKey(vectorObj);
            default:
                throw new UnknowAttrType("Unknown attribute type in extractKeyFromTuple");
        }
    }
    
    /**
     * Get the next RID from the current index scan
     */
    private RID getNextRIDFromScan() throws Exception {
        if (currentIndexScan == null) {
            return null;
        }
        
        try {
            if (currentIndexScan instanceof BTFileScan) {
                KeyDataEntry entry = ((BTFileScan)currentIndexScan).get_next();
                if (entry == null) return null;
                return ((LeafData)entry.data).getData();
            } else if (currentIndexScan instanceof java.util.Iterator) {
                // Use fully qualified name for Java's Iterator
                java.util.Iterator<RID> iter = (java.util.Iterator<RID>)currentIndexScan;
                if (iter.hasNext()) {
                    return iter.next();
                } else {
                    return null;
                }
            }
        } catch (Exception e) {
            System.err.println("Error getting next RID from scan: " + e.getMessage());
            closeCurrentScan();
            throw e;
        }
        
        return null;
    }
    
    /**
     * Close the current index scan if it exists
     */
    private void closeCurrentScan() {
        if (currentIndexScan != null) {
            try {
                if (currentIndexScan instanceof BTFileScan) {
                    ((BTFileScan)currentIndexScan).DestroyBTreeFileScan();
                }
                // No need to close a List iterator
            } catch (Exception e) {
                System.err.println("Error closing scan: " + e.getMessage());
            }
            currentIndexScan = null;
        }
    }

    /**
     * Implement the abstract method close() from super class Iterator
     * to finish cleaning up
     * 
     * @exception IOException    I/O error from lower layers
     * @exception JoinsException join error from lower layers
     * @exception IndexException index access error
     */
    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {
            try {
                outer.close();
            } catch (Exception e) {
                throw new JoinsException(e, "Outer iterator close failed.");
            }
            
            closeCurrentScan();
            
            // Clean up persistent index resources
            if (currentBTreeFile != null) {
                try {
                    currentBTreeFile.close();
                    currentBTreeFile = null;
                } catch (Exception e) {
                    System.err.println("Error closing BTree file: " + e.getMessage());
                }
            }
            
            // LSH index doesn't need explicit closing, but set to null to allow GC
            currentLSHIndex = null;
            
            closeFlag = true;
        }
    }
}