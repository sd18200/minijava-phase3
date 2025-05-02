package index;
import global.*;
import bufmgr.*;
import diskmgr.*; 
import btree.*;
import iterator.*;
import heap.*; 
import java.io.*;


/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class IndexScan extends Iterator {

  /**
   * class constructor. set up the index scan.
   * @param index type of the index (B_Index, Hash)
   * @param relName name of the input relation
   * @param indName name of the input index
   * @param types array of types in this relation
   * @param str_sizes array of string sizes (for attributes that are string)
   * @param noInFlds number of fields in input tuple
   * @param noOutFlds number of fields in output tuple
   * @param outFlds fields to project
   * @param selects conditions to apply, first one is primary
   * @param fldNum field number of the indexed field
   * @param indexOnly whether the answer requires only the key or the tuple
   * @exception IndexException error from the lower layer
   * @exception InvalidTypeException tuple type not valid
   * @exception InvalidTupleSizeException tuple size not valid
   * @exception UnknownIndexTypeException index type unknown
   * @exception IOException from the lower layer
   */
  public IndexScan(
	   IndexType     index,        
	   final String  relName,  
	   final String  indName,  
	   AttrType      types[],      
	   short         str_sizes[],     
	   int           noInFlds,          
	   int           noOutFlds,         
	   FldSpec       outFlds[],     
	   CondExpr      selects[],  
	   final int     fldNum,
	   final boolean indexOnly
	   ) 
    throws IndexException, 
	   InvalidTypeException,
	   InvalidTupleSizeException,
	   UnknownIndexTypeException,
	   IOException
  {
    _fldNum = fldNum;
    _noInFlds = noInFlds;
    _types = types;
    _s_sizes = str_sizes;
    
    AttrType[] Jtypes = new AttrType[noOutFlds];
    short[] ts_sizes;
    Jtuple = new Tuple();
    
    try {
      ts_sizes = TupleUtils.setup_op_tuple(Jtuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
    }
    catch (TupleUtilsException e) {
      throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from TupleUtils.setup_op_tuple()");
    }
    catch (InvalidRelation e) {
      throw new IndexException(e, "IndexScan.java: InvalidRelation caught from TupleUtils.setup_op_tuple()");
    }
     
    _selects = selects;
    perm_mat = outFlds;
    _noOutFlds = noOutFlds;
    tuple1 = new Tuple();    
    try {
      tuple1.setHdr((short) noInFlds, types, str_sizes);
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile error");
    }
    
    t1_size = tuple1.size();
    index_only = indexOnly;  // added by bingjie miao
    
    try {
      f = new Heapfile(relName);
    }
    catch (Exception e) {
      throw new IndexException(e, "IndexScan.java: Heapfile not created");
    }
    
    switch(index.indexType) {
      // linear hashing is not yet implemented
    case IndexType.B_Index:
      // error check the select condition
      // must be of the type: value op symbol || symbol op value
      // but not symbol op symbol || value op value
      try {
	indFile = new BTreeFile(indName); 
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
      }
      
      try {
	indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
      }
      catch (Exception e) {
	throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
      }
      
      break;
    case IndexType.None:
    default:
      throw new UnknownIndexTypeException("Only BTree index is supported so far");
      
    }
    
  }
  
  /**
   * returns the next tuple.
   * if <code>index_only</code>, only returns the key value 
   * (as the first field in a tuple)
   * otherwise, retrive the tuple and returns the whole tuple
   * @return the tuple
   * @exception IndexException error from the lower layer
   * @exception UnknownKeyTypeException key type unknown
   * @exception IOException from the lower layer
   */
  public Tuple get_next()
    throws IndexException,
           UnknownKeyTypeException,
           IOException
  {
    RID rid = null; // Initialize RID
    KeyDataEntry nextentry = null; // Initialize nextentry

    try { // Outer try for the whole method logic

        // --- Get next entry from BTree ---
        try {
            nextentry = indScan.get_next(); // Call BTFileScan.get_next()
        } catch (ScanIteratorException e) { // Catch specific BTree scan exception
            System.err.println("ERROR: IndexScan.get_next(): ScanIteratorException from indScan.get_next(): " + e.getMessage()); // ADDED
            e.printStackTrace(); // ADDED
            throw new IndexException(e, "Error getting next entry from BTFileScan");
        } catch (Exception e) { // Catch unexpected runtime errors from indScan.get_next()
            System.err.println("ERROR: IndexScan.get_next(): Unexpected Exception from indScan.get_next(): " + e.getMessage()); // ADDED
            e.printStackTrace(); // ADDED
            throw new IndexException(e, "Unexpected error in indScan.get_next()");
        }

        // Loop while there are entries from the index scan
        while(nextentry != null) {

            // --- Handle index_only case ---
            if (index_only) {
                // only need to return the key
                AttrType[] attrType = new AttrType[1];
                short[] s_sizes = new short[1]; // Note: Size might need adjustment for strings

                if (_types[_fldNum -1].attrType == AttrType.attrInteger) {
                  attrType[0] = new AttrType(AttrType.attrInteger);
                  s_sizes[0] = 4; // Size of integer
                  try {
                    Jtuple.setHdr((short) 1, attrType, s_sizes);
                    Jtuple.setIntFld(1, ((IntegerKey)nextentry.key).getKey().intValue());
                  } catch (Exception e) {
                    System.err.println("ERROR: IndexScan.get_next(): Exception setting index_only Integer tuple: " + e.getMessage()); // ADDED
                    e.printStackTrace(); // ADDED
                    throw new IndexException(e, "IndexScan.java: Error setting index_only Integer tuple");
                  }
                } else if (_types[_fldNum -1].attrType == AttrType.attrString) {
                  attrType[0] = new AttrType(AttrType.attrString);
                  // calculate string size of _fldNum - This logic might be fragile if _s_sizes isn't just for strings
                  int count = 0;
                  int strSizeIndex = -1;
                  for (int i=0; i<_fldNum; i++) { // Iterate up to _fldNum (exclusive) as it's 1-based index
                    if (i < _types.length && _types[i].attrType == AttrType.attrString) { // Check bounds for _types
                        if (i == _fldNum - 1) { // If this string field IS the indexed field
                           strSizeIndex = count; // Index in _s_sizes corresponds to the count-th string
                        }
                        count++;
                    }
                  }
                  // Validate calculated index against _s_sizes array bounds
                  if (strSizeIndex < 0 || _s_sizes == null || strSizeIndex >= _s_sizes.length) {
                       System.err.println("ERROR: IndexScan.get_next(): Invalid string size index calculation for index_only. Index=" + strSizeIndex + ", _s_sizes length=" + (_s_sizes == null ? "null" : _s_sizes.length)); // ADDED
                       throw new IndexException(null, "IndexScan.java: Invalid string size index calculation for index_only.");
                  }
                  s_sizes[0] = _s_sizes[strSizeIndex]; // Get the correct string size

                  try {
                    Jtuple.setHdr((short) 1, attrType, s_sizes);
                    Jtuple.setStrFld(1, ((StringKey)nextentry.key).getKey());
                  } catch (Exception e) {
                     System.err.println("ERROR: IndexScan.get_next(): Exception setting index_only String tuple: " + e.getMessage()); // ADDED
                     e.printStackTrace(); // ADDED
                    throw new IndexException(e, "IndexScan.java: Error setting index_only String tuple");
                  }
                } else {
                  // attrReal not supported for now
                  System.err.println("ERROR: IndexScan.get_next(): index_only requested for unsupported type: " + _types[_fldNum -1].attrType); // ADDED
                  throw new UnknownKeyTypeException("Only Integer and String keys are supported for index_only scan");
                }
                return Jtuple; // Return the key-only tuple
            } // End index_only block

            // --- Full Tuple Retrieval and Processing (not index_only) ---

            // Extract the RID from the KeyDataEntry
            try {
                rid = ((LeafData) nextentry.data).getData();
            } catch (ClassCastException e) {
                System.err.println("ERROR: IndexScan.get_next(): ClassCastException casting entry data to LeafData for key " + nextentry.key); // ADDED
                e.printStackTrace(); // ADDED
                throw new IndexException(e, "Error casting entry data to LeafData");
            } catch (Exception e) { // Catch other potential errors during data access
                System.err.println("ERROR: IndexScan.get_next(): Exception getting RID from entry data for key " + nextentry.key + ": " + e.getMessage()); // ADDED
                e.printStackTrace(); // ADDED
                throw new IndexException(e, "Error getting RID from entry data");
            }

            // Retrieve the full tuple from the heap file using the RID
            try {
                if (f == null) {
                     System.err.println("CRITICAL ERROR: IndexScan.get_next(): Heapfile 'f' is null!"); // ADDED
                     throw new IndexException(null, "Internal error: Heapfile 'f' is null in IndexScan");
                }
                tuple1 = f.getRecord(rid); // Fetch the record into tuple1

                if (tuple1 == null) {
                    // This might happen if the record was deleted between index entry creation and now
                    System.err.println("Warning: IndexScan.get_next(): RID " + rid + " from index points to a deleted/invalid record in heapfile. Trying next entry."); // ADDED
                    // Continue to the next index entry by getting it at the end of the loop
                } else {

                    // Set header for the retrieved tuple (using the base relation's schema)
                    if (_types == null) {
                         System.err.println("CRITICAL ERROR: IndexScan.get_next(): Base relation schema '_types' is null!"); // ADDED
                         throw new IndexException(null, "Internal error: Base relation schema (_types) not set in IndexScan");
                    }
                    tuple1.setHdr((short) _noInFlds, _types, _s_sizes); // Set header on tuple1

                    // --- Evaluate Predicates ---
                    boolean eval = false; // Initialize eval
                    try {
                        // Assuming _selects might be null if no predicates are applied after index lookup
                        if (_selects != null) {
                             eval = PredEval.Eval(_selects, tuple1, null, _types, null);
                        } else {
                             eval = true; // If no selects, the tuple passes
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR: IndexScan.get_next(): Exception during predicate evaluation: " + e.getMessage()); // ADDED
                        e.printStackTrace(); // ADDED
                        throw new IndexException(e, "IndexScan.java: Error during predicate evaluation");
                    }

                    // --- Projection (if predicate passed) ---
                    if (eval) {
                        // Check projection info
                        if (perm_mat == null) { // Assuming perm_mat holds projection info (outFlds)
                             System.err.println("CRITICAL ERROR: IndexScan.get_next(): Projection info (perm_mat) is null!"); // ADDED
                             throw new IndexException(null, "Internal error: Projection info (perm_mat) not set in IndexScan");
                        }
                        // Jtuple should already have its header set up in the constructor by TupleUtils.setup_op_tuple

                        try {
                            Projection.Project(tuple1, _types, Jtuple, perm_mat, _noOutFlds); // Project tuple1 into Jtuple
                        } catch (Exception e) { // Catch FieldNumberOutOfBoundException, etc.
                            System.err.println("ERROR: IndexScan.get_next(): Exception during projection: " + e.getMessage()); // ADDED
                            e.printStackTrace(); // ADDED
                            throw new IndexException(e, "Error during projection");
                        }

                        return Jtuple; // Return the projected tuple
                    } else {
                         // If eval is false, loop continues to get the next entry from indScan below
                    }
                } // End if (tuple1 != null)
            } catch (Exception e) { // Catch HFException, HFBufMgrException, InvalidSlotNumberException, etc. from getRecord or setHdr
                System.err.println("ERROR: IndexScan.get_next(): Exception retrieving record or setting header for RID " + rid + ": " + e.getMessage()); // ADDED
                e.printStackTrace(); // ADDED
                throw new IndexException(e, "Error retrieving record from heap file or setting header for RID " + rid);
            }

            // If we reach here, it means either:
            // 1. f.getRecord(rid) returned null (deleted tuple)
            // 2. Predicate evaluation failed (eval == false)
            // In both cases, we need to get the next entry from the index scan.
            try {
                nextentry = indScan.get_next();
            } catch (Exception e) {
                System.err.println("ERROR: IndexScan.get_next(): Exception calling indScan.get_next() at end of loop: " + e.getMessage()); // ADDED
                e.printStackTrace(); // ADDED
                throw new IndexException(e, "IndexScan.java: BTree error at end of loop");
            }

        } // End while(nextentry != null)

        return null; // End of scan

    } catch (Exception finalE) { // Catch any unexpected exception thrown within the method logic
        System.err.println("ERROR: IndexScan.get_next(): UNEXPECTED EXCEPTION caught by outer handler: " + finalE.getMessage()); // ADDED
        finalE.printStackTrace(); // Log the exception that caused premature exit
        // Wrap unexpected runtime exceptions
        throw new IndexException(finalE, "Unexpected exception wrapper in IndexScan.get_next()");
    }
  }
  
  /**
   * Cleaning up the index scan.
   * Unpins the leaf page currently pinned by the scan (via DestroyBTreeFileScan)
   * and the header page pinned by the BTreeFile object (via indFile.close()).
   * Does not remove either the original relation or the index from the database.
   * @exception IndexException error from the lower layer (can wrap other exceptions)
   * @exception IOException from the lower layer (potentially from DestroyBTreeFileScan)
   * @exception JoinsException (Assuming Iterator.close() throws this)
   * @exception SortException (Assuming Iterator.close() throws this)
   * @exception InvalidRelation (Assuming Iterator.close() throws this)
   * @exception BufmgrException (Assuming Iterator.close() might throw a general buffer exception)
   * Note: We remove the specific bufmgr exceptions from throws and wrap them.
   */

  public void close()
    throws IOException,
      IndexException
  {
    if (!closeFlag) {
        Exception scanCloseException = null; // To store exception from DestroyBTreeFileScan
        Exception fileCloseException = null; // To store exception from indFile.close()

        try {
            // --- 1. Close the underlying IndexFileScan (e.g., BTFileScan) ---
            if (indScan != null) {
                try {
                    if (indScan instanceof BTFileScan) {
                        ((BTFileScan) indScan).DestroyBTreeFileScan();
                    } else {
                    }
                } catch (IOException | PageUnpinnedException | InvalidFrameNumberException | HashEntryNotFoundException | ReplacerException e) {
                    // Catch specific exceptions that DestroyBTreeFileScan might throw
                    scanCloseException = e; // Store the exception
                    System.err.println("ERROR: IndexScan.close() exception during DestroyBTreeFileScan: " + e.getMessage());
                } catch (Exception e) { // Catch any other unexpected runtime exceptions
                     scanCloseException = e;
                     System.err.println("ERROR: IndexScan.close() UNEXPECTED exception during DestroyBTreeFileScan: " + e.getMessage());
                } finally {
                    indScan = null; // Release reference even if close failed
                }
            } else {
            }

            // --- 2. Close the IndexFile (e.g., BTreeFile) ---
            if (indFile != null) {
                try {
                    if (indFile instanceof BTreeFile) {
                        ((BTreeFile) indFile).close();
                    } else {
                    }
                } catch (PageUnpinnedException | InvalidFrameNumberException | HashEntryNotFoundException | ReplacerException e) {
                     // Catch specific exceptions that BTreeFile.close might throw
                    fileCloseException = e; // Store the exception
                    System.err.println("ERROR: IndexScan.close() exception during indFile.close(): " + e.getMessage());
                } catch (Exception e) { // Catch any other unexpected runtime exceptions
                     fileCloseException = e;
                     System.err.println("ERROR: IndexScan.close() UNEXPECTED exception during indFile.close(): " + e.getMessage());
                } finally {
                    indFile = null; // Release reference even if close failed
                }
            } else {
            }

        } finally {
            closeFlag = true; // Ensure flag is set regardless of exceptions during close attempts

            // --- 3. Handle and re-throw exceptions ---
            // Prioritize throwing the exception from closing the BTreeFile (header page)
            if (fileCloseException != null) {
                if (scanCloseException != null) {
                    System.err.println("ERROR: IndexScan.close() also encountered error during scan cleanup (DestroyBTreeFileScan): " + scanCloseException.getMessage());
                }
                // --- WRAP specific bufmgr exceptions in IndexException ---
                if (fileCloseException instanceof PageUnpinnedException ||
                    fileCloseException instanceof InvalidFrameNumberException ||
                    fileCloseException instanceof HashEntryNotFoundException ||
                    fileCloseException instanceof ReplacerException) {
                     throw new IndexException(fileCloseException, "Buffer manager error closing index file (BTreeFile/header page).");
                }
                // Wrap other exceptions from indFile.close()
                throw new IndexException(fileCloseException, "Error closing index file (BTreeFile/header page).");

            } else if (scanCloseException != null) {
                // If only the scan close failed, handle that exception
                // Rethrow IOException directly if allowed by Iterator.close()
                if (scanCloseException instanceof IOException) throw (IOException) scanCloseException;

                // --- WRAP specific bufmgr exceptions in IndexException ---
                if (scanCloseException instanceof PageUnpinnedException ||
                    scanCloseException instanceof InvalidFrameNumberException ||
                    scanCloseException instanceof HashEntryNotFoundException ||
                    scanCloseException instanceof ReplacerException) {
                     throw new IndexException(scanCloseException, "Buffer manager error closing index scan (DestroyBTreeFileScan/leaf page).");
                }
                // Wrap others
                throw new IndexException(scanCloseException, "Error closing index scan (DestroyBTreeFileScan/leaf page).");
            }
            // If no exceptions occurred, the finally block completes normally.
        }
    } else {
    }
  }
  
  public FldSpec[]      perm_mat;
  private IndexFile     indFile;
  private IndexFileScan indScan;
  private AttrType[]    _types;
  private short[]       _s_sizes; 
  private CondExpr[]    _selects;
  private int           _noInFlds;
  private int           _noOutFlds;
  private Heapfile      f;
  private Tuple         tuple1;
  private Tuple         Jtuple;
  private int           t1_size;
  private int           _fldNum;       
  private boolean       index_only;    

}

