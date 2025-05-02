package iterator;

import java.io.*;
import java.util.ArrayList; // Added for ArrayList
import java.util.Arrays;    // Added for Arrays.sort
import java.util.Comparator;// Added for Comparator
import java.util.LinkedList; // Added for LinkedList
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import index.*;
import chainexception.*;

/**
 * The Sort class sorts a file. All necessary information are passed as
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get tuples in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class Sort extends Iterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 10; // Default initial size for temp files array

  private AttrType[]  _in;
  private short       n_cols;
  private short[]     str_lens;
  private Iterator    _am;
  private int         _sort_fld;
  private TupleOrder  order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         Nruns; // Actual number of runs generated
  private int         max_elems_in_heap; // Calculated max tuples for run generation heaps
  private int         sortFldLen;
  private int         tuple_size;

  private pnodeSplayPQ Q; // Main priority queue for merge or internal sort results
  private Heapfile[]   temp_files; // Array to hold temporary run files
  private int          n_tempfiles; // Current allocated size of temp_files array
  private Tuple        output_tuple; // Temporary holder for tuple returned by delete_min
  private int[]        n_tuples; // Number of tuples in each run
  // private int          n_runs; // Seems redundant with Nruns and n_tempfiles capacity logic
  private Tuple        op_buf; // Output buffer tuple
  private OBuf         o_buf; // Output buffer for writing runs
  private SpoofIbuf[]  i_buf; // Input buffers for reading runs during merge
  private PageId[]     bufs_pids; // Page IDs for allocated buffer pages
  private boolean useBM = true; // flag for whether to use buffer manager
  private boolean closeFlag = false; // Flag to prevent double closing

  private Vector100Dtype Target; // Target vector for attrVector100D distance sort
  private int k; // Number of top tuples to return (0 means all)
  private int tuples_returned; // Counter for top-k returned tuples

  /**
   * Original constructor for backward compatibility (no vector sort).
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the tuples
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_fld_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  public Sort(AttrType[] in, short len_in, short[] str_sizes,
             Iterator am, int sort_fld, TupleOrder sort_order,
             int sort_fld_len, int n_pages)
             throws IOException, SortException {

      // Call the extended constructor with null for Target and 0 for k
      this(in, len_in, str_sizes, am, sort_fld, sort_order, sort_fld_len, n_pages, null, 0);
  }

  /**
   * Class constructor, take information about the tuples, and set up
   * the sorting. Includes parameters for vector distance sorting.
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the tuples
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_fld_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @param Target target vector for distance calculation (can be null)
   * @param k number of nearest tuples to return (0 means all tuples)
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  public Sort(AttrType[] in,
          short      len_in,
          short[]    str_sizes,
          Iterator   am,
          int        sort_fld,
          TupleOrder sort_order,
          int        sort_fld_len,
          int        n_pages,
          Vector100Dtype Target,
          int k
          ) throws IOException, SortException
  {
    // Save the Target and k parameters
    this.Target = Target;
    this.k = k;
    if (this.k < 0) this.k = 0; // Ensure k is non-negative

    _in = new AttrType[len_in];
    n_cols = len_in;
    int n_strs = 0;

    for (int i=0; i<len_in; i++) {
      _in[i] = new AttrType(in[i].attrType);
      if (in[i].attrType == AttrType.attrString) {
        n_strs ++;
      }
    }

    str_lens = new short[n_strs];

    n_strs = 0;
    for (int i=0; i<len_in; i++) {
      if (_in[i].attrType == AttrType.attrString) {
        str_lens[n_strs] = str_sizes[n_strs];
        n_strs ++;
      }
    }

    Tuple t = new Tuple(); // need Tuple.java
    try {
      t.setHdr(len_in, _in, str_sizes);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: t.setHdr() failed");
    }
    tuple_size = t.size();

    _am = am;
    _sort_fld = sort_fld;
    order = sort_order;
    _n_pages = n_pages; // Store n_pages

    // Allocate buffer pages
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
        get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
        throw new SortException(e, "Sort.java: BUFmgr error");
      }
    }
    else {
      for (int i=0; i<_n_pages; i++) bufs[i] = new byte[MAX_SPACE];
    }

    first_time = true;
    tuples_returned = 0; // Initialize top-k counter
    Nruns = 0; // Initialize actual number of runs

    // Initialize temporary file structures
    temp_files = new Heapfile[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_tuples = new int[ARBIT_RUNS];
    // n_runs = ARBIT_RUNS; // Removed, use Nruns for actual count, n_tempfiles for capacity

    // Do not create temp_files[0] here, it will be created on demand
    // try {
    //   temp_files[0] = new Heapfile(null); // First temp file is created here
    // }
    // catch (Exception e) {
    //   throw new SortException(e, "Sort.java: Heapfile error");
    // }

    o_buf = new OBuf(); // Initialize output buffer object
    // o_buf.init(...) // Initialization moved to where it's first needed

    // *** FIX: Calculate max_elems_in_heap based on available memory ***
    // Estimate how many tuples can fit in the available buffer pages for the heap.
    // Subtract a few pages for safety/overhead (input buffer, output buffer, heap file directory pages).
    int pages_for_heap = Math.max(1, _n_pages - 3); // Use at least 1 page, reserve some for I/O
    max_elems_in_heap = (int) Math.floor((double) pages_for_heap * GlobalConst.MINIBASE_PAGESIZE / tuple_size);
    if (max_elems_in_heap <= 0) {
        // Ensure at least a minimal heap size if calculation is too small (e.g., tiny buffer pool)
        max_elems_in_heap = 1; // Or throw an error if insufficient memory
        System.err.println("Warning: Calculated max_elems_in_heap is <= 0. Setting to 1. Check buffer pool size and tuple size.");
    }
    System.out.println("DEBUG: Sort Constructor - Calculated max_elems_in_heap = " + max_elems_in_heap);
    // *** END FIX ***

    sortFldLen = sort_fld_len;

    // Initialize the main merge queue Q (used for both internal sort results and merge phase)
    AttrType sortAttrType = (_sort_fld > 0 && _sort_fld <= _in.length) ? _in[_sort_fld - 1] : null;
    if (sortAttrType == null) {
         try { close(); } catch (Exception ce) {} // Clean up before throwing
         throw new SortException("Invalid sort field number: " + _sort_fld);
    }
    // Use the constructor that accepts Target
    Q = new pnodeSplayPQ(_sort_fld, sortAttrType, order, this.Target); // Pass Target

    // Initialize the output tuple buffer
    op_buf = new Tuple(tuple_size);   // need Tuple.java
    try {
      op_buf.setHdr(n_cols, _in, str_lens);
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: op_buf.setHdr() failed");
    }
  }

  /**
   * Returns the next tuple in sorted order.
   * Note: You need to copy out the content of the tuple, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next tuple, null if all tuples exhausted or k limit reached
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception UnknowAttrType attribute type unknown
   * @exception LowMemException memory low exception
   * @exception Exception other exceptions
   */
  public Tuple get_next()
    throws IOException,
       SortException,
       UnknowAttrType,
       LowMemException,
       JoinsException,
       Exception
  {
    if (first_time) {
      // first get_next call to the sort routine
      first_time = false;
      tuples_returned = 0; // Initialize counter

      // Determine sort attribute type early
      AttrType sortAttrType = (_sort_fld > 0 && _sort_fld <= _in.length) ? _in[_sort_fld - 1] : null;
      if (sortAttrType == null) {
          try { close(); } catch (Exception ce) {} // Clean up before throwing
          throw new SortException("Invalid sort field number: " + _sort_fld);
      }

      // Initialize structures for internal sort (assuming it fits initially)
      // Use a temporary LinkedList to buffer tuples initially
      LinkedList<Tuple> internalBuffer = new LinkedList<>();
      Tuple tuple;
      int tups_read = 0;
      boolean external_sort_needed = false;

      // Estimate max tuples that can fit in memory buffer pages
      // Use the calculated max_elems_in_heap as the threshold for internal buffer size
      int max_tuples_in_mem = max_elems_in_heap;
      System.out.println("DEBUG: Sort.get_next() - Using max_tuples_in_mem = " + max_tuples_in_mem +
                         " (based on calculated max_elems_in_heap)");

      // Read tuples from input iterator
      while ((tuple = _am.get_next()) != null) {
          tups_read++;
          Tuple tupleCopy = new Tuple(tuple); // Make a copy

          // Check if we've exceeded the memory capacity
          if (max_tuples_in_mem > 0 && tups_read > max_tuples_in_mem) {
              System.out.println("DEBUG: Sort.get_next() - Buffer full! tups_read=" + tups_read +
                                 ", max_tuples_in_mem=" + max_tuples_in_mem +
                                 ". Triggering external sort.");
              external_sort_needed = true;

              // *** FIX: Write buffered tuples to the first run ***
              try {
                  // Ensure temp_files array is initialized and large enough for run 0
                  if (temp_files == null || n_tempfiles == 0) {
                      throw new SortException("Sort.java: temp_files array not initialized before external sort trigger.");
                  }
                  // Create the first temp file
                  temp_files[0] = new Heapfile(null);
                  Nruns = 1; // We are creating the first run

                  // Add the tuple that caused the overflow to the buffer
                  internalBuffer.add(tupleCopy);

                  // Sort the tuples currently in internalBuffer
                  Tuple[] bufferedTuples = internalBuffer.toArray(new Tuple[0]);
                  internalBuffer = null; // Release memory

                  // Use a custom comparator for sorting tuples in memory
                  Arrays.sort(bufferedTuples, new TupleComparator(order.tupleOrder == TupleOrder.Ascending,
                                                                  _sort_fld, sortAttrType, Target));

                  // Write sorted tuples to the first run file
                  // Initialize o_buf for the first run file
                  o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
                  n_tuples[0] = 0; // Initialize tuple count for run 0
                  for (Tuple t : bufferedTuples) {
                      o_buf.Put(t);
                      n_tuples[0]++; // Count tuples in the first run
                  }
                  o_buf.flush(); // Flush the first run
                  System.out.println("DEBUG: Sort.get_next() - Wrote " + n_tuples[0] + " buffered tuples to first run file: " + temp_files[0].get_fileName());

                  // Generate remaining runs starting from run 1 (index 1)
                  // Pass the calculated max_elems_in_heap
                  int remainingRuns = generate_runs(max_elems_in_heap, sortAttrType, sortFldLen, 1); // Pass starting run index 1
                  Nruns += remainingRuns; // Add the number of newly generated runs

                  // Setup for merge phase
                  setup_for_merge(tuple_size, Nruns);

              } catch (Exception e) {
                  // Print stack trace for detailed debugging
                  e.printStackTrace();
                  throw new SortException(e, "Sort.java: Error writing first run or generating subsequent runs");
              }
              // *** END FIX ***
              break; // Exit the read loop after handling overflow
          } else {
              // Still fits in memory, add to internal buffer list
              internalBuffer.add(tupleCopy);
          }
      } // End while reading input

      System.out.println("DEBUG: Sort.get_next() - Finished reading input. Total tuples read: " + tups_read);

      if (!external_sort_needed) {
          System.out.println("DEBUG: Sort.get_next() - Performing internal sort. All " + tups_read + " tuples fit in memory.");
          // All tuples fit in memory, sort the internal buffer and load into Q
          Nruns = 0; // Explicitly mark as internal sort

          if (internalBuffer != null && !internalBuffer.isEmpty()) {
              Tuple[] sortedTuples = internalBuffer.toArray(new Tuple[0]);
              internalBuffer = null; // Release memory

              // Use a custom comparator for sorting tuples in memory
              Arrays.sort(sortedTuples, new TupleComparator(order.tupleOrder == TupleOrder.Ascending,
                                                            _sort_fld, sortAttrType, Target));

              // Load sorted tuples into the main priority queue Q
              for (Tuple t : sortedTuples) {
                  pnode node = new pnode();
                  node.tuple = t; // No copy needed here as it's already sorted
                  // Calculate distance if needed
                  if (sortAttrType.attrType == AttrType.attrVector100D && Target != null) {
                      try {
                          int[] vector = t.getVectorFld(_sort_fld);
                          node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
                      } catch (Exception e) {
                          throw new SortException(e, "Sort.java: Error calculating distance for internal sort load");
                      }
                  } else {
                      node.distance = -1;
                  }
                  try {
                      Q.enq(node); // Enqueue into the main queue
                  } catch (Exception e) {
                      throw new SortException(e, "Sort.java: Error enqueuing node during internal sort load");
                  }
              }
          }
          // Ensure op_buf is initialized for returning tuples
          if (op_buf == null) op_buf = new Tuple(tuple_size);
          try { op_buf.setHdr(n_cols, _in, str_lens); } catch (Exception e) { throw new SortException(e, "Sort.java: op_buf.setHdr() failed"); }
          i_buf = null; // No input buffers needed
          o_buf = null; // Ensure o_buf (for runs) is null

      } else if (Nruns > 0) {
          // External sort was triggered and runs were generated
          System.out.println("DEBUG: Sort.get_next() - External sort finished generation. Setting up for merge of " + Nruns + " runs.");
          // Ensure op_buf is initialized for returning tuples
          if (op_buf == null) op_buf = new Tuple(tuple_size);
          try { op_buf.setHdr(n_cols, _in, str_lens); } catch (Exception e) { throw new SortException(e, "Sort.java: op_buf.setHdr() failed"); }
          // setup_for_merge was already called in the external sort transition block
      } else {
          // External sort triggered but generate_runs produced 0 runs (e.g., empty input after overflow point)
          System.out.println("DEBUG: Sort.get_next() - External sort triggered but 0 runs generated (or only 1 run created).");
          // If Nruns is 1, setup_for_merge might still be needed if the first run was written
          if (Nruns == 1 && temp_files != null && temp_files[0] != null) {
               System.out.println("DEBUG: Sort.get_next() - Only one run generated, setting up to read from it.");
               setup_for_merge(tuple_size, Nruns); // Setup to read from the single run
          } else {
               return null; // No data to return
          }
      }

    } // End if (first_time)

    // *** FIX: Check k limit *before* retrieving the next tuple ***
    if (k > 0 && tuples_returned >= k) {
        // Already returned k tuples, stop iteration.
        return null;
    }

    // Check if merge queue (Q) is initialized and not empty
    // Q is now used for both internal and external results
    if (Q == null || Q.empty()) {
      // no more tuples available
      return null;
    }

    output_tuple = delete_min(); // Get the next tuple in sorted order (from internalQ or merge)

    if (output_tuple != null){
      // We got a tuple, increment the counter and return a copy
      tuples_returned++; // *** Increment counter ***
      // Ensure op_buf is ready (should have been initialized in first_time block)
      if (op_buf == null) {
           throw new SortException("Sort.java: op_buf is null when returning tuple");
      }
      op_buf.tupleCopy(output_tuple); // Use op_buf for returning copy
      return op_buf;
    }
    else {
      // delete_min returned null, signifies end of data
      return null;
    }
  }  // End get_next


  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min/max)
   * from each run into a heap. <code>delete_min() </code> will then get
   * the minimum/maximum of all runs.
   * @param tuple_size size (in bytes) of each tuple
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to
   *                 sort in two passes (a subclass of SortException).
   * @exception SortException something went wrong in the lower layer.
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int tuple_size, int n_R_runs)
    throws IOException,
       LowMemException,
       SortException,
       Exception
  {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages)
      throw new LowMemException("Sort.java: Not enough memory for merge input buffers ("+ n_R_runs +" runs > "+_n_pages+" pages)");

    int i;
    pnode cur_node;  // need pq_defs.java

    i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
    // Allocate one buffer page per run for input.
    // Ensure we don't try to use more pages than available.
    int pages_per_run = Math.max(1, _n_pages / n_R_runs); // Simple allocation, might need refinement

    for (i=0; i<n_R_runs; i++) {
      i_buf[i] = new SpoofIbuf();

      // Assign buffer pages to this run's input buffer
      // This simple approach gives each run one page from the start of bufs array.
      // A more robust approach might manage page allocation more carefully.
      byte[][] apage = new byte[1][];
      if (i < _n_pages) { // Ensure we don't go out of bounds
          apage[0] = bufs[i];
      } else {
          // This case should be prevented by the check at the start, but handle defensively
          throw new LowMemException("Sort.java: Ran out of buffer pages during merge setup.");
      }

      // Initialize the input buffer for the current run
      // Ensure temp_files[i] and n_tuples[i] are valid
      if (temp_files == null || i >= temp_files.length || temp_files[i] == null) {
          throw new SortException("Sort.java: Invalid temporary file reference for run " + i);
      }
      if (n_tuples == null || i >= n_tuples.length) {
           throw new SortException("Sort.java: Invalid tuple count reference for run " + i);
      }

      i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);

      cur_node = new pnode();
      cur_node.run_num = i;

      // Get the first tuple from the run
      Tuple temp_tuple = new Tuple(tuple_size);
      try {
        temp_tuple.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
        throw new SortException(e, "Sort.java: Tuple.setHdr() failed in setup_for_merge");
      }

      temp_tuple = i_buf[i].Get(temp_tuple);

      if (temp_tuple != null) {
        // Calculate distance if needed for the first element from the run
        AttrType sortAttrType = _in[_sort_fld - 1];
        if (sortAttrType.attrType == AttrType.attrVector100D && Target != null) {
            try {
                int[] vector = temp_tuple.getVectorFld(_sort_fld);
                cur_node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: Error calculating distance in setup_for_merge");
            }
        } else {
            cur_node.distance = -1; // Indicate distance not applicable or calculated
        }

        cur_node.tuple = temp_tuple; // no copy needed
        try {
          Q.enq(cur_node); // Enqueue into the main merge queue
        }
        catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq() in setup_for_merge");
        }
        catch (TupleUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq() in setup_for_merge");
        }
      } else {
          // Handle case where a run might be empty (shouldn't happen if generate_runs is correct)
          System.err.println("Warning: Sort.setup_for_merge - Run " + i + " appears to be empty.");
      }
    }
    return;
  }


  /**
   * Generate sorted runs starting from a specific index.
   * Reads tuples from the input iterator (_am) and writes them into sorted runs on disk.
   * Uses replacement selection with two heaps.
   * @param max_elems maximum number of elements in each heap (calculated based on memory)
   * @param sortFldType attribute type of the sort field
   * @param sortFldLen length of the sort field
   * @param start_run_index The index (0-based) of the run file to start writing to.
   * @return number of *new* runs generated by this call
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen, int start_run_index)
  throws IOException,
         SortException,
         UnknowAttrType,
         TupleUtilsException,
         JoinsException,
         Exception {
    Tuple tuple = null; // Initialize tuple to null
    pnode cur_node;
    // Initialize run-generation priority queues with Target vector
    pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, sortFldType, order, this.Target);
    pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, sortFldType, order, this.Target);

    pnodeSplayPQ pcurr_Q = Q1; // Heap for elements fitting in the current run
    pnodeSplayPQ pother_Q = Q2; // Heap for elements belonging to the next run
    Tuple lastElem = new Tuple(tuple_size); // Stores the last element written to the current run
    try {
        lastElem.setHdr(n_cols, _in, str_lens);
    } catch (Exception e) {
        throw new SortException(e, "Sort.java: setHdr() failed for lastElem");
    }

    int run_num = start_run_index; // Start from the specified run index
    int runs_generated_this_call = 0; // Track runs created by this specific call
    int p_elems_curr_Q = 0; // Number of elements in the current heap
    int p_elems_other_Q = 0; // Number of elements in the other heap
    int comp_res;
    double lastElemDistance = -1; // Store distance of lastElem for vector comparison

    // Set the lastElem to be the minimum/maximum value for the sort field to start the first run
    if (order.tupleOrder == TupleOrder.Ascending) {
        try { MIN_VAL(lastElem, sortFldType); } catch (Exception e) { throw new SortException(e, "MIN_VAL failed"); }
        lastElemDistance = Double.NEGATIVE_INFINITY; // For vector distance comparison
    } else {
        try { MAX_VAL(lastElem, sortFldType); } catch (Exception e) { throw new SortException(e, "MAX_VAL failed"); }
        lastElemDistance = Double.POSITIVE_INFINITY; // For vector distance comparison
    }
    // Note: Initial distance calculation for MIN/MAX vector might be needed if comparison logic requires it.

    // Ensure temp_files and n_tuples arrays are large enough for the starting index
    if (run_num >= n_tempfiles) {
        expand_temp_arrays(run_num + 1); // Expand to accommodate at least the starting run index
    }

    // Setup the output buffer for the first run to be generated by this call
    try {
        // Create heap file for the current run if it doesn't exist
        if (temp_files[run_num] == null) {
             temp_files[run_num] = new Heapfile(null);
             System.out.println("DEBUG: Sort.generate_runs - Created temp file for run " + run_num + ": " + temp_files[run_num].get_fileName());
        } else {
             System.out.println("DEBUG: Sort.generate_runs - Using existing temp file for run " + run_num + ": " + temp_files[run_num].get_fileName());
        }
        // Initialize output buffer for this run
        o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
        n_tuples[run_num] = 0; // Initialize tuple count for this run
    } catch (Exception e) {
        throw new SortException(e, "Sort.java: Heapfile error or OBuf init failed for run " + run_num);
    }


    // Main loop: Read input, manage heaps, write runs
    while (true) {
        // Try to read the next tuple from the input iterator
        try {
            tuple = _am.get_next();
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: get_next() failed during run generation");
        }

        if (tuple != null) {
            // Process the new tuple
            cur_node = new pnode();
            cur_node.tuple = new Tuple(tuple); // Copy needed

            // Calculate distance if vector sort
            if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                try {
                    int[] vector = cur_node.tuple.getVectorFld(_sort_fld);
                    cur_node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
                } catch (Exception e) {
                    throw new SortException(e, "Sort.java: Error calculating distance for new tuple");
                }
            } else {
                cur_node.distance = -1;
            }

            // Compare the new tuple with the last element written to the current run
            if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                comp_res = Double.compare(cur_node.distance, lastElemDistance);
            } else {
                comp_res = TupleUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);
            }

            // Decide whether the new tuple fits in the current run or belongs to the next run
            if ((comp_res < 0 && order.tupleOrder == TupleOrder.Ascending) ||
                (comp_res > 0 && order.tupleOrder == TupleOrder.Descending)) {
                // Tuple doesn't fit in the current run, add to the 'other' heap
                if (p_elems_other_Q == max_elems) {
                    // This case should ideally not happen with replacement selection logic below
                    // If it does, it means both heaps are full, which needs careful handling.
                    // For simplicity, we might throw an error or handle based on specific strategy.
                    throw new SortException("Sort.java: generate_runs - Both heaps full, unexpected state.");
                }
                try {
                    pother_Q.enq(cur_node);
                } catch (Exception e) { throw new SortException(e, "Sort.java: error enqueuing node to other queue"); }
                p_elems_other_Q++;
            } else {
                // Tuple fits in the current run, add to the 'current' heap
                if (p_elems_curr_Q == max_elems) {
                    // Current heap is full, need to extract the min/max element to write
                    pnode node_to_write = pcurr_Q.deq(); // Get the best element for the current run
                    p_elems_curr_Q--;

                    // Write this element to the output buffer
                    o_buf.Put(node_to_write.tuple);
                    n_tuples[run_num]++;

                    // Update lastElem and its distance
                    TupleUtils.SetValue(lastElem, node_to_write.tuple, _sort_fld, sortFldType);
                    if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                        lastElemDistance = node_to_write.distance;
                    }

                    // Now add the new input tuple to the current heap
                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (Exception e) { throw new SortException(e, "Sort.java: error enqueuing new node after replacement"); }
                    p_elems_curr_Q++;
                } else {
                    // Current heap is not full, just add the new tuple
                    try {
                        pcurr_Q.enq(cur_node);
                    } catch (Exception e) { throw new SortException(e, "Sort.java: error enqueuing new node to current queue"); }
                    p_elems_curr_Q++;
                }
            }
        } else {
            // Input iterator is exhausted (tuple == null)
            // Break the loop to proceed to flushing remaining elements
            break;
        }

        // Check if the current heap is empty (can happen if max_elems is small or input pattern matches)
        // If empty, and the other heap has elements, switch heaps to start a new run.
        if (p_elems_curr_Q == 0 && p_elems_other_Q > 0) {
            // Flush the (now finished) current run
            if (n_tuples[run_num] > 0 || o_buf.GetNumOfTuples() > 0) { // Only count if data was written
                 n_tuples[run_num] = (int) o_buf.flush();
                 runs_generated_this_call++;
            } else {
                 // If the run was empty, potentially reuse the file slot? Or just mark count as 0.
                 n_tuples[run_num] = 0;
            }
            run_num++;

            // Expand arrays if needed
            if (run_num >= n_tempfiles) {
                expand_temp_arrays(run_num + 1);
            }

            // Create new heap file for the next run
            try {
                temp_files[run_num] = new Heapfile(null);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: create Heapfile failed on switch");
            }

            // Initialize output buffer for the new run
            o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
            n_tuples[run_num] = 0; // Initialize count for the new run

            // Reset lastElem for the new run
            if (order.tupleOrder == TupleOrder.Ascending) {
                try { MIN_VAL(lastElem, sortFldType); } catch (Exception e) { throw new SortException(e, "MIN_VAL failed"); }
                lastElemDistance = Double.NEGATIVE_INFINITY;
            } else {
                try { MAX_VAL(lastElem, sortFldType); } catch (Exception e) { throw new SortException(e, "MAX_VAL failed"); }
                lastElemDistance = Double.POSITIVE_INFINITY;
            }

            // Switch the queues
            pnodeSplayPQ tempQ = pcurr_Q;
            pcurr_Q = pother_Q;
            pother_Q = tempQ;
            int tempelems = p_elems_curr_Q;
            p_elems_curr_Q = p_elems_other_Q;
            p_elems_other_Q = tempelems;
        }

    } // End of while(true) loop reading input

    // --- Input exhausted, flush remaining elements ---

    // Flush remaining elements from the current heap to the current run buffer
    while (p_elems_curr_Q > 0) {
        pnode node_to_write = pcurr_Q.deq();
        o_buf.Put(node_to_write.tuple);
        n_tuples[run_num]++;
        p_elems_curr_Q--;
    }
    // Flush the current run buffer if it contains data
    if (n_tuples[run_num] > 0 || o_buf.GetNumOfTuples() > 0) {
         n_tuples[run_num] = (int) o_buf.flush();
         runs_generated_this_call++; // Count this run
    } else {
         n_tuples[run_num] = 0; // Mark as empty if nothing was written
    }


    // Write remaining elements from the other heap to a *new* run
    if (p_elems_other_Q > 0) {
        run_num++;
        runs_generated_this_call++;

        // Expand arrays if needed
        if (run_num >= n_tempfiles) {
            expand_temp_arrays(run_num + 1);
        }

        // Create new heap file & init o_buf for the final run
        try {
            temp_files[run_num] = new Heapfile(null);
            o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);
            n_tuples[run_num] = 0; // Initialize count
        } catch (Exception e) { throw new SortException(e, "Sort.java: create Heapfile/OBuf init failed for final run"); }

        // Write elements from the other heap (they are already sorted relative to each other)
        while (p_elems_other_Q > 0) {
            pnode node_to_write = pother_Q.deq();
            o_buf.Put(node_to_write.tuple);
            n_tuples[run_num]++;
            p_elems_other_Q--;
        }
        n_tuples[run_num] = (int) o_buf.flush(); // Flush the final run
    }

    System.out.println("DEBUG: Sort.generate_runs(start=" + start_run_index + ") - Finished. New runs generated: " + runs_generated_this_call);
    return runs_generated_this_call; // Return the count of *new* runs created by this call
}


  /**
   * Remove the minimum value (or maximum for descending) among all the runs or from the internal sort queue.
   * @return the next tuple in sorted order, null if exhausted.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  private Tuple delete_min()
    throws IOException,
       SortException,
       Exception
  {
    pnode cur_node;                // Node from the priority queue
    Tuple new_tuple, old_tuple;

    cur_node = Q.deq(); // Dequeue the next best tuple (uses pnodeCMP implicitly)
    if (cur_node == null) { // Check if the main queue was empty
        return null;
    }
    old_tuple = cur_node.tuple; // This is the tuple to be returned

    // If this was an external sort (Nruns > 0), try to refill the queue
    // from the run the dequeued tuple came from.
    if (Nruns > 0) {
        // Check if i_buf is initialized (should be if Nruns > 0 and setup_for_merge was called)
        if (i_buf == null) {
             throw new SortException("Sort.java: delete_min inconsistency - Nruns > 0 but i_buf is null");
        }
        // Check if run_num is valid for the i_buf array
        if (cur_node.run_num < 0 || cur_node.run_num >= Nruns || cur_node.run_num >= i_buf.length) {
             throw new SortException("Sort.java: delete_min inconsistency - invalid run_num " + cur_node.run_num + " for Nruns=" + Nruns);
        }

        // Check if the input buffer for this run is valid and not exhausted
        if (i_buf[cur_node.run_num] != null && i_buf[cur_node.run_num].empty() != true) {
            // Run not exhausted, get the next tuple from this run's input buffer
            new_tuple = new Tuple(tuple_size); // Allocate space for the new tuple
            try {
                new_tuple.setHdr(n_cols, _in, str_lens);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: setHdr() failed in delete_min refill");
            }

            new_tuple = i_buf[cur_node.run_num].Get(new_tuple); // Read the next tuple

            if (new_tuple != null) {
                // Successfully read a new tuple, prepare the node and enqueue it
                // Calculate distance if needed
                AttrType sortAttrType = _in[_sort_fld - 1];
                if (sortAttrType.attrType == AttrType.attrVector100D && Target != null) {
                    try {
                        int[] vector = new_tuple.getVectorFld(_sort_fld);
                        cur_node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
                    } catch (Exception e) {
                        throw new SortException(e, "Sort.java: Error calculating distance in delete_min refill");
                    }
                } else {
                    cur_node.distance = -1;
                }

                cur_node.tuple = new_tuple;  // Update the node with the new tuple
                try {
                    Q.enq(cur_node); // Enqueue the node back into the main merge queue
                } catch (UnknowAttrType e) {
                    throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq() in delete_min");
                } catch (TupleUtilsException e) {
                    throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq() in delete_min");
                }
            } else {
                // Get() returned null, indicating the run is now exhausted.
                // Do nothing, the queue will not be refilled from this run.
                System.err.println("********** Debug: Run " + cur_node.run_num + " exhausted in delete_min ***************");
            }
        } // end if run not exhausted
    } // End of Nruns > 0 block (external sort refill logic)

    // Return the tuple that was originally dequeued
    return old_tuple;
  }


  /**
   * Set lastElem to be the minimum value of the appropriate type
   * @param lastElem the tuple
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MIN_VAL(Tuple lastElem, AttrType sortFldType)
    throws IOException,
       FieldNumberOutOfBoundException,
       UnknowAttrType {

    // char[] c = new char[1]; // Not needed for standard types
    // c[0] = Character.MIN_VALUE;
    // String s = new String(c); // Use empty string or specific low-value string if needed

    switch (sortFldType.attrType) {
    case AttrType.attrInteger:
      lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
      break;
    case AttrType.attrReal:
      lastElem.setFloFld(_sort_fld, Float.NEGATIVE_INFINITY); // Use NEGATIVE_INFINITY for float min
      break;
    case AttrType.attrString:
      lastElem.setStrFld(_sort_fld, ""); // Empty string is often used as a practical minimum
      break;
    case AttrType.attrVector100D:
      // Representing a true minimum vector is tricky. Fill with MIN_VALUE.
      int[] minVector = new int[100];
      java.util.Arrays.fill(minVector, Integer.MIN_VALUE); // Use Arrays.fill
      lastElem.setVectorFld(_sort_fld, minVector);
      break;
    default:
      // attrSymbol or attrNull cannot be used as sort keys directly in this context
      throw new UnknowAttrType("Sort.java: cannot determine MIN_VAL for attrSymbol or attrNull");
    }
  }

  /**
   * Set lastElem to be the maximum value of the appropriate type
   * @param lastElem the tuple
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MAX_VAL(Tuple lastElem, AttrType sortFldType)
    throws IOException,
       FieldNumberOutOfBoundException,
       UnknowAttrType {

    // char[] c = new char[1]; // Not reliable for max string
    // c[0] = Character.MAX_VALUE;
    // String s = new String(c); // Max string representation is complex

    switch (sortFldType.attrType) {
    case AttrType.attrInteger:
      lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
      break;
    case AttrType.attrReal:
      lastElem.setFloFld(_sort_fld, Float.POSITIVE_INFINITY); // Use POSITIVE_INFINITY for float max
      break;
    case AttrType.attrString:
      // Representing a true maximum string is difficult. Often a string of high-value chars is used,
      // but its length dependency makes it unreliable. A practical approach might depend on context
      // or assuming a maximum length and filling with high chars. For simplicity, we might
      // rely on the comparison logic handling nulls or specific end markers if needed.
      // Setting a very long string might exceed field limits. Let's use a placeholder.
      // Consider if a specific "max" value is defined for your string comparisons.
      // lastElem.setStrFld(_sort_fld, s); // Avoid using single MAX_VALUE char
       throw new UnknowAttrType("Sort.java: MAX_VAL for String is ambiguous/not implemented reliably.");
      // break; // If implemented, break here
    case AttrType.attrVector100D:
      // Fill with MAX_VALUE.
      int[] maxVector = new int[100];
      java.util.Arrays.fill(maxVector, Integer.MAX_VALUE); // Use Arrays.fill
      lastElem.setVectorFld(_sort_fld, maxVector);
      break;
    default:
      throw new UnknowAttrType("Sort.java: cannot determine MAX_VAL for attrSymbol or attrNull");
    }
  }

  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  public void close() throws SortException, IOException
  {
    // clean up
    if (!closeFlag) {
      closeFlag = true; // Set flag immediately to prevent re-entry issues

      try {
        if (_am != null) _am.close();
      }
      catch (Exception e) {
        throw new SortException(e, "Sort.java: error in closing input iterator.");
      }

      if (useBM) {
        try {
          if (bufs_pids != null) free_buffer_pages(_n_pages, bufs_pids);
        }
        catch (Exception e) {
          throw new SortException(e, "Sort.java: BUFmgr error");
        }
        // Invalidate page IDs in local array (optional, helps debugging)
        // if (bufs_pids != null) {
        //     for (int i=0; i<_n_pages; i++) {
        //         if (bufs_pids[i] != null) bufs_pids[i].pid = INVALID_PAGE;
        //     }
        // }
      }

      // Close and delete temporary run files
      if (temp_files != null) {
          // Iterate up to Nruns (the actual number of runs created)
          for (int i = 0; i < Nruns; i++) {
            if (temp_files[i] != null) {
              try {
                System.out.println("DEBUG: Sort.close() - Deleting temp file: " + temp_files[i].get_fileName());
                temp_files[i].deleteFile();
              }
              catch (Exception e) {
                // Log or print warning instead of throwing exception during close
                System.err.println("Sort.java: Warning - error deleting temp file " + i + ": " + e.getMessage());
                e.printStackTrace(); // Print stack trace for more info
              }
              temp_files[i] = null; // Help garbage collection
            }
          }
      }

      // Nullify references to help GC
      Q = null;
      _in = null;
      str_lens = null;
      _am = null;
      bufs = null;
      temp_files = null;
      n_tuples = null;
      op_buf = null;
      o_buf = null;
      i_buf = null;
      bufs_pids = null;
      Target = null;
    }
  }


  /**
   * Internal helper method to expand temporary file arrays when needed.
   * @param required_size The minimum required capacity after expansion.
   */
  private void expand_temp_arrays(int required_size) {
      int new_size = Math.max(n_tempfiles * 2, required_size); // Double the size or meet requirement
      System.out.println("DEBUG: Sort.expand_temp_arrays - Expanding to size: " + new_size);

      Heapfile[] temp1 = new Heapfile[new_size];
      System.arraycopy(temp_files, 0, temp1, 0, n_tempfiles);
      temp_files = temp1;

      int[] temp2 = new int[new_size];
      // Copy only up to the current capacity (n_tempfiles) or actual runs (Nruns)?
      // Copying up to n_tempfiles seems safer if n_tuples was sized with it.
      System.arraycopy(n_tuples, 0, temp2, 0, n_tempfiles);
      n_tuples = temp2;

      n_tempfiles = new_size; // Update the capacity tracker
      // n_runs = new_size; // Removed, Nruns tracks actual runs
  }


  // --- Buffer Management Helper Methods --- (Copied from example, may need adjustment)

  /**
   * Gets buffer pages from the buffer manager.
   * @param n_pages the number of buffer pages required
   * @param PageIds buffer page IDs array (output)
   * @param bufs buffer frames array (output)
   * @exception IteratorBMException exception from buffer manager
   */
  public void get_buffer_pages(int n_pages, PageId[] PageIds, byte[][] bufs)
    throws IteratorBMException
  {
    Page pgptr = new Page();
    PageId pgid = null;

    for(int i=0; i < n_pages; i++) {
      try {
        pgid = SystemDefs.JavabaseBM.newPage(pgptr,1);
      }
      catch (Exception e) {
        throw new IteratorBMException(e, "Sort.java: BUFmgr error");
      }

      if(pgid == null)
        throw new IteratorBMException(null, "Sort.java: BUFmgr error");

      PageIds[i] = new PageId(pgid.pid);
      bufs[i] = pgptr.getpage(); // Should be pgptr.getpage() or similar method to get byte array reference

    }
  }

  /**
   * Frees buffer pages from the buffer manager.
   * @param n_pages the number of buffer pages to free
   * @param PageIds buffer page IDs array
   * @exception IteratorBMException exception from buffer manager
   */
  public void free_buffer_pages(int n_pages, PageId[] PageIds)
    throws IteratorBMException
  {
    for (int i=0; i<n_pages; i++) {
      if (PageIds[i] != null && PageIds[i].pid != INVALID_PAGE) { // Check for null and validity
          try {
            SystemDefs.JavabaseBM.freePage(PageIds[i]);
          }
          catch (Exception e) {
            throw new IteratorBMException(e, "Sort.java: BUFmgr error");
          }
          PageIds[i].pid = INVALID_PAGE; // Mark as invalid after freeing
      }
    }
  }


  /**
   * Comparator class for sorting Tuples in memory during the initial phase
   * or for internal sort.
   */
  private class TupleComparator implements Comparator<Tuple> {
      private boolean ascending;
      private int sortField;
      private AttrType sortAttrType;
      private Vector100Dtype targetVector; // Target for distance comparison

      public TupleComparator(boolean ascending, int sortField, AttrType sortAttrType, Vector100Dtype targetVector) {
          this.ascending = ascending;
          this.sortField = sortField;
          this.sortAttrType = sortAttrType;
          this.targetVector = targetVector;
      }

      @Override
      public int compare(Tuple t1, Tuple t2) {
          try {
              int comp_res;
              if (sortAttrType.attrType == AttrType.attrVector100D && targetVector != null) {
                  // Compare based on distance to targetVector
                  int[] v1 = t1.getVectorFld(sortField);
                  int[] v2 = t2.getVectorFld(sortField);
                  double dist1 = TupleUtils.calculateEuclideanDistance(v1, targetVector.getValues());
                  double dist2 = TupleUtils.calculateEuclideanDistance(v2, targetVector.getValues());
                  comp_res = Double.compare(dist1, dist2);
              } else {
                  // Compare based on field value
                  comp_res = TupleUtils.CompareTupleWithTuple(sortAttrType, t1, sortField, t2, sortField);
              }

              return ascending ? comp_res : -comp_res; // Adjust for descending order

          } catch (Exception e) {
              // Handle exceptions during comparison, e.g., log error or throw runtime exception
              System.err.println("Error during tuple comparison: " + e.getMessage());
              e.printStackTrace();
              return 0; // Or throw a runtime exception
          }
      }
  }


} // End Class Sort