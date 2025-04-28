package iterator;

import java.io.*;
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
  private static final int ARBIT_RUNS = 10;

  private AttrType[]  _in;
  private short       n_cols;
  private short[]     str_lens;
  private Iterator    _am;
  private int         _sort_fld;
  private TupleOrder  order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         Nruns;
  private int         max_elems_in_heap;
  private int         sortFldLen;
  private int         tuple_size;

  private pnodeSplayPQ Q;
  private Heapfile[]   temp_files;
  private int          n_tempfiles;
  private Tuple        output_tuple;
  private int[]        n_tuples;
  private int          n_runs;
  private Tuple        op_buf;
  private OBuf         o_buf;
  private SpoofIbuf[]  i_buf;
  private PageId[]     bufs_pids;
  private boolean useBM = true; // flag for whether to use buffer manager

  private Vector100Dtype Target; // Target vector for attrVector100D
  private int k; // Number of top tuples to maintain
  private int tuples_returned; // Counter for top-k

  /**
   * Original constructor for backward compatibility
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
      throw new LowMemException("Sort.java: Not enough memory to sort in two passes.");

    int i;
    pnode cur_node;  // need pq_defs.java

    i_buf = new SpoofIbuf[n_R_runs];   // need io_bufs.java
    for (int j=0; j<n_R_runs; j++) i_buf[j] = new SpoofIbuf();

    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98

    for (i=0; i<n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      i_buf[i].init(temp_files[i], apage, 1, tuple_size, n_tuples[i]);

      cur_node = new pnode();
      cur_node.run_num = i;

      // may need change depending on whether Get() returns the original
      // or make a copy of the tuple, need io_bufs.java ???
      Tuple temp_tuple = new Tuple(tuple_size);

      try {
        temp_tuple.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
        throw new SortException(e, "Sort.java: Tuple.setHdr() failed");
      }

      temp_tuple = i_buf[i].Get(temp_tuple);  // need io_bufs.java

      if (temp_tuple != null) {
        /*
        System.out.print("Get tuple from run " + i);
        temp_tuple.print(_in);
        */
        // Calculate distance if needed for the first element from the run
        if (_in[_sort_fld - 1].attrType == AttrType.attrVector100D && Target != null) {
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
          Q.enq(cur_node); // Q uses pnodeCMP which handles distance comparison
        }
        catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        }
        catch (TupleUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
        }

      }
    }
    return;
  }


  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @param  sortFldType  attribute type of the sort field
   * @param  sortFldLen   length of the sort field
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen)
  throws IOException,
         SortException,
         UnknowAttrType,
         TupleUtilsException,
         JoinsException,
         Exception {
    Tuple tuple = null; // Initialize tuple to null to avoid potential uninitialized variable warning
    pnode cur_node;
    // *** FIX: Initialize run-generation queues with Target ***
    pnodeSplayPQ Q1 = new pnodeSplayPQ(_sort_fld, sortFldType, order, this.Target); // NEW
    pnodeSplayPQ Q2 = new pnodeSplayPQ(_sort_fld, sortFldType, order, this.Target); // NEW

    pnodeSplayPQ pcurr_Q = Q1;
    pnodeSplayPQ pother_Q = Q2;
    Tuple lastElem = new Tuple(tuple_size);  // need tuple.java
    try {
        lastElem.setHdr(n_cols, _in, str_lens);
    } catch (Exception e) {
        throw new SortException(e, "Sort.java: setHdr() failed");
    }

    int run_num = 0;  // keeps track of the number of runs
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    int comp_res;
    double lastElemDistance = -1; // Store distance for vector comparison

    // Set the lastElem to be the minimum/maximum value for the sort field
    if (order.tupleOrder == TupleOrder.Ascending) {
        try {
            MIN_VAL(lastElem, sortFldType);
        } catch (Exception e) { // Catch broader exceptions
            throw new SortException(e, "MIN_VAL failed");
        }
    } else {
        try {
            MAX_VAL(lastElem, sortFldType);
        } catch (Exception e) { // Catch broader exceptions
            throw new SortException(e, "MAX_VAL failed");
        }
    }
    // Pre-calculate distance for lastElem if vector sort
    if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
        try {
            int[] lastVec = lastElem.getVectorFld(_sort_fld);
            lastElemDistance = TupleUtils.calculateEuclideanDistance(lastVec, Target.getValues());
        } catch (Exception e) {
            // Handle case where MIN/MAX_VAL might not produce a valid vector initially
            System.err.println("Warning: Could not calculate initial distance for MIN/MAX_VAL vector.");
            lastElemDistance = (order.tupleOrder == TupleOrder.Ascending) ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
    }


    // Fill the initial heap(s) up to max_elems
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
        try {
            tuple = _am.get_next();  // according to Iterator.java
        } catch (Exception e) {
            throw new SortException(e, "Sort.java: get_next() failed");
        }

        if (tuple == null) {
            break; // End of input stream
        }
        cur_node = new pnode();
        cur_node.tuple = new Tuple(tuple); // tuple copy needed

        // Calculate distance for attrVector100D
        if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
            try {
                int[] vector = cur_node.tuple.getVectorFld(_sort_fld);
                cur_node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: Error calculating distance during initial fill");
            }
        } else {
            cur_node.distance = -1; // Indicate distance not applicable
        }

        // Enqueue into the current heap (uses corrected pnodeCMP)
        try {
            pcurr_Q.enq(cur_node);
        } catch (Exception e) { // Catch broader exceptions
            throw new SortException(e, "Sort.java: error enqueuing node during initial fill");
        }
        p_elems_curr_Q++;

        // Optional: Top-k optimization (still potentially incorrect logic, but kept as was)
        // if (k > 0 && p_elems_curr_Q > k) {
        //   pcurr_Q.deq(); // This removes min/max based on sort order, not necessarily worst for top-k
        //   p_elems_curr_Q--;
        // }
    }

    // Main loop: Dequeue, compare, write to run or enqueue to other heap
    while (true) {
        cur_node = pcurr_Q.deq(); // Uses corrected pnodeCMP implicitly
        if (cur_node == null) {
            // Current queue is empty. Check if we need to switch or if we're done.
            if (p_elems_other_Q == 0) {
                // Both queues are empty, break the main loop
                break;
            } else {
                // Switch queues and start a new run
                // (Logic for switching queues is handled later in the loop)
                // Need to ensure tuple is null so we don't try to read input when switching
                tuple = null;
            }
        } else {
            p_elems_curr_Q--; // Decrement count for the dequeued element

            // Compare the dequeued element with the last element written to the current run
            if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                // Compare distances (cur_node.distance was calculated on enq/read)
                comp_res = Double.compare(cur_node.distance, lastElemDistance);
            } else {
                // Compare using TupleUtils for non-vector types
                comp_res = TupleUtils.CompareTupleWithValue(sortFldType, cur_node.tuple, _sort_fld, lastElem);
            }

            // Check if the current node fits in the current run based on sort order
            if ((comp_res < 0 && order.tupleOrder == TupleOrder.Ascending) ||
                (comp_res > 0 && order.tupleOrder == TupleOrder.Descending)) {
                // Element doesn't fit in the current run, enqueue to the other queue
                try {
                    pother_Q.enq(cur_node); // Uses corrected pnodeCMP
                } catch (Exception e) { // Catch broader exceptions
                    throw new SortException(e, "Sort.java: error enqueuing node to other queue");
                }
                p_elems_other_Q++;
            } else {
                // Element fits, write it to the output buffer for the current run
                o_buf.Put(cur_node.tuple);
                // Update lastElem and its distance for the next comparison
                TupleUtils.SetValue(lastElem, cur_node.tuple, _sort_fld, sortFldType);
                if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                    lastElemDistance = cur_node.distance; // Update distance
                }
            }
        } // End of processing dequeued node


        // Try to fill the current queue with a new tuple from input
        // This happens if the current queue is not full OR if we just switched queues
        // AND the input iterator (_am) is not exhausted (tuple != null check needed)
        if (p_elems_curr_Q < max_elems) {
             try {
                // Only get next if we haven't already determined input is exhausted
                // (e.g., if cur_node was null, tuple might already be null)
                if (cur_node != null || p_elems_other_Q == 0) { // Avoid reading if just switched and other queue has elements
                   tuple = _am.get_next();
                } else {
                   tuple = null; // Assume input exhausted if we switched due to empty current queue
                }
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: get_next() failed while refilling");
            }

            if (tuple != null) {
                // Got a new tuple, prepare and enqueue it
                pnode new_node = new pnode();
                new_node.tuple = new Tuple(tuple); // Copy needed

                // Calculate distance if vector sort
                if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                    try {
                        int[] vector = new_node.tuple.getVectorFld(_sort_fld);
                        new_node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
                    } catch (Exception e) {
                         throw new SortException(e, "Sort.java: Error calculating distance for new node");
                    }
                } else {
                    new_node.distance = -1;
                }

                // Enqueue into the current heap
                try {
                    pcurr_Q.enq(new_node); // Uses corrected pnodeCMP
                } catch (Exception e) { // Catch broader exceptions
                    throw new SortException(e, "Sort.java: error enqueuing new node");
                }
                p_elems_curr_Q++;
            }
            // If tuple is null, input is exhausted, do nothing here, loop will continue until queues are empty.
        }


        // Check if it's time to switch runs (either other queue is full or current queue is empty and input exhausted)
        // Need to check if tuple is null to confirm input exhaustion when p_elems_curr_Q is 0
        // The warning "The local variable tuple may not have been initialized" is addressed by initializing 'tuple' to null at declaration.
        if (p_elems_other_Q == max_elems || (p_elems_curr_Q == 0 && tuple == null && p_elems_other_Q > 0)) {
            // Flush the current run
            n_tuples[run_num] = (int) o_buf.flush();
            run_num++;

            // Expand arrays if needed
            if (run_num == n_tempfiles) {
                Heapfile[] temp1 = new Heapfile[2 * n_tempfiles];
                System.arraycopy(temp_files, 0, temp1, 0, n_tempfiles);
                temp_files = temp1;
                n_tempfiles *= 2;

                int[] temp2 = new int[2 * n_runs];
                System.arraycopy(n_tuples, 0, temp2, 0, n_runs);
                n_tuples = temp2;
                n_runs *= 2;
            }

            // Create new heap file for the next run
            try {
                temp_files[run_num] = new Heapfile(null);
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: create Heapfile failed");
            }

            // Initialize output buffer for the new run
            o_buf.init(bufs, _n_pages, tuple_size, temp_files[run_num], false);

            // Reset lastElem for the new run
            if (order.tupleOrder == TupleOrder.Ascending) {
                try { MIN_VAL(lastElem, sortFldType); } catch (Exception e) { throw new SortException(e, "MIN_VAL failed"); }
            } else {
                try { MAX_VAL(lastElem, sortFldType); } catch (Exception e) { throw new SortException(e, "MAX_VAL failed"); }
            }
            // Reset lastElemDistance
            if (sortFldType.attrType == AttrType.attrVector100D && Target != null) {
                 try {
                    int[] lastVec = lastElem.getVectorFld(_sort_fld);
                    lastElemDistance = TupleUtils.calculateEuclideanDistance(lastVec, Target.getValues());
                } catch (Exception e) {
                    System.err.println("Warning: Could not calculate distance for MIN/MAX_VAL vector on run switch.");
                    lastElemDistance = (order.tupleOrder == TupleOrder.Ascending) ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                }
            }

            // Switch the queues
            pnodeSplayPQ tempQ = pcurr_Q;
            pcurr_Q = pother_Q;
            pother_Q = tempQ;
            int tempelems = p_elems_curr_Q;
            p_elems_curr_Q = p_elems_other_Q;
            p_elems_other_Q = tempelems;

            // After switching, ensure tuple is null so we don't try to read input immediately if pcurr_Q is not full
            tuple = null;
        }

        // If current queue is empty AND input is exhausted AND other queue is empty, the loop condition (cur_node == null && p_elems_other_Q == 0) will handle exit.

    } // End of while(true)

    // Flush the very last run if any elements were processed
    if (run_num > 0 || n_tuples[0] > 0 || o_buf.GetNumOfTuples() > 0) { // Check if anything was written
        n_tuples[run_num] = (int) o_buf.flush();
        run_num++;
    }
    return run_num;
}

  /**
   * Remove the minimum value among all the runs.
   * @return the minimum tuple removed
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer.
   */
  private Tuple delete_min()
    throws IOException,
       SortException,
       Exception
  {
    pnode cur_node;                // needs pq_defs.java
    Tuple new_tuple, old_tuple;

    cur_node = Q.deq(); // Uses corrected pnodeCMP implicitly
    if (cur_node == null) { // Check if queue was empty
        return null;
    }
    old_tuple = cur_node.tuple;
    /*
    System.out.print("Get ");
    old_tuple.print(_in);
    */
    // we just removed one tuple from one run, now we need to put another
    // tuple of the same run into the queue
    if (i_buf[cur_node.run_num].empty() != true) {
      // run not exhausted
      new_tuple = new Tuple(tuple_size); // need tuple.java??

      try {
        new_tuple.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
        throw new SortException(e, "Sort.java: setHdr() failed");
      }

      new_tuple = i_buf[cur_node.run_num].Get(new_tuple);
      if (new_tuple != null) {
        /*
        System.out.print(" fill in from run " + cur_node.run_num);
        new_tuple.print(_in);
        */
        // Calculate distance if needed for the newly read tuple
        if (_in[_sort_fld - 1].attrType == AttrType.attrVector100D && Target != null) {
            try {
                int[] vector = new_tuple.getVectorFld(_sort_fld);
                cur_node.distance = TupleUtils.calculateEuclideanDistance(vector, Target.getValues());
            } catch (Exception e) {
                throw new SortException(e, "Sort.java: Error calculating distance in delete_min");
            }
        } else {
            cur_node.distance = -1;
        }

        cur_node.tuple = new_tuple;  // no copy needed -- I think Bingjie 4/22/98
        try {
          Q.enq(cur_node); // Uses corrected pnodeCMP
        } catch (UnknowAttrType e) {
          throw new SortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
        } catch (TupleUtilsException e) {
          throw new SortException(e, "Sort.java: TupleUtilsException caught from Q.enq()");
        }
      }
      else {
        // This case should ideally not happen if empty() check is reliable,
        // but handle defensively.
        System.err.println("********** Warning: i_buf[" + cur_node.run_num + "].Get() returned null despite not being empty ***************");
      }

    }

    // changed to return Tuple instead of return char array ????
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

    char[] c = new char[1];
    c[0] = Character.MIN_VALUE;
    String s = new String(c);

    switch (sortFldType.attrType) {
    case AttrType.attrInteger:
      lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
      break;
    case AttrType.attrReal:
      lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
      break;
    case AttrType.attrString:
      lastElem.setStrFld(_sort_fld, s);
      break;
    case AttrType.attrVector100D:
      int[] minVector = new int[100];
      java.util.Arrays.fill(minVector, Integer.MIN_VALUE); // Use Arrays.fill
      lastElem.setVectorFld(_sort_fld, minVector);
      break;
    default:
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }

    return;
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

    char[] c = new char[1];
    c[0] = Character.MAX_VALUE;
    String s = new String(c);

    switch (sortFldType.attrType) {
    case AttrType.attrInteger:
      lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
      break;
    case AttrType.attrReal:
      lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
      break;
    case AttrType.attrString:
      lastElem.setStrFld(_sort_fld, s);
      break;
    case AttrType.attrVector100D:
      int[] maxVector = new int[100];
      java.util.Arrays.fill(maxVector, Integer.MAX_VALUE); // Use Arrays.fill
      lastElem.setVectorFld(_sort_fld, maxVector);
      break;
    default:
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }

    return;
  }

  /**
   * Class constructor, take information about the tuples, and set up
   * the sorting
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the tuples
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_field_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @param Target target vector for distance calculation
   * @param k number of nearest tuples to maintain (0 means all tuples)
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
    _n_pages = n_pages;

    // this may need change, bufs ???  need io_bufs.java
    //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
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

    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new Heapfile[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_tuples = new int[ARBIT_RUNS];
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new Heapfile(null); // First temp file is created here
    }
    catch (Exception e) {
      throw new SortException(e, "Sort.java: Heapfile error");
    }

    o_buf = new OBuf();

    o_buf.init(bufs, _n_pages, tuple_size, temp_files[0], false);
    //    output_tuple = null;

    max_elems_in_heap = 200; // Or calculate based on buffer pages/tuple size
    sortFldLen = sort_fld_len;

    // *** FIX: Initialize main merge queue Q with Target ***
    AttrType sortAttrType = (_sort_fld > 0 && _sort_fld <= _in.length) ? _in[_sort_fld - 1] : null;
    if (sortAttrType == null) {
         try { close(); } catch (Exception ce) {} // Clean up before throwing
         throw new SortException("Invalid sort field number: " + _sort_fld);
    }
    // Use the constructor that accepts Target
    Q = new pnodeSplayPQ(_sort_fld, sortAttrType, order, this.Target); // NEW


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

      // generate runs
      AttrType sortAttrType = (_sort_fld > 0 && _sort_fld <= _in.length) ? _in[_sort_fld - 1] : null;
       if (sortAttrType == null) {
           close(); // Clean up before throwing
           throw new SortException("Invalid sort field number: " + _sort_fld);
       }
      Nruns = generate_runs(max_elems_in_heap, sortAttrType, sortFldLen);
      // System.out.println("Generated " + Nruns + " runs");

      // setup state to perform merge of runs.
      if (Nruns > 0) { // Only setup merge if runs were generated
        setup_for_merge(tuple_size, Nruns);
      } else {
         // No runs generated (empty input or all filtered out)
         // Allow get_next to return null naturally
         return null; // No runs means no tuples
      }
    }

    // *** FIX: Check k limit *before* retrieving the next tuple ***
    if (k > 0 && tuples_returned >= k) {
        // Already returned k tuples, stop iteration.
        return null;
    }

    // Check if merge queue is initialized and not empty
    if (Q == null || Q.empty()) {
      // no more tuples available from the merge phase
      return null;
    }

    output_tuple = delete_min(); // Get the next tuple in sorted order

    if (output_tuple != null){
      // We got a tuple, increment the counter and return a copy
      tuples_returned++; // *** Increment counter ***
      op_buf.tupleCopy(output_tuple);
      return op_buf;
    }
    else {
      // delete_min returned null, signifies end of data
      return null;
    }
  } // End get_next


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

      try {
        if (_am != null) _am.close(); // Check if _am is null before closing
      }
      catch (Exception e) {
        throw new SortException(e, "Sort.java: error in closing input iterator.");
      }

      if (useBM) {
        try {
          if (bufs_pids != null) free_buffer_pages(_n_pages, bufs_pids); // Check if bufs_pids is null
        }
        catch (Exception e) {
          throw new SortException(e, "Sort.java: BUFmgr error");
        }
        // It's safer to null-check bufs_pids before iterating
        if (bufs_pids != null) {
            for (int i=0; i<_n_pages; i++) {
                if (bufs_pids[i] != null) bufs_pids[i].pid = INVALID_PAGE;
            }
        }
      }

      // It's safer to null-check temp_files before iterating
      if (temp_files != null) {
          for (int i = 0; i<temp_files.length; i++) {
            if (temp_files[i] != null) {
              try {
                temp_files[i].deleteFile();
              }
              catch (Exception e) {
                // Log or print warning instead of throwing exception during close
                System.err.println("Sort.java: Warning - error deleting temp file: " + e.getMessage());
                // throw new SortException(e, "Sort.java: Heapfile error"); // Avoid throwing from close if possible
              }
              temp_files[i] = null;
            }
          }
      }
      closeFlag = true;
    }
  }
}