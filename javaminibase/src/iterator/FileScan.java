package iterator;
   

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


import java.lang.*;
import java.io.*;

/**
 *open a heapfile and according to the condition expression to get
 *output file, call get_next to get all tuples
 */
public class FileScan extends  Iterator
{
  private AttrType[] _in1;
  private short in1_len;
  private short[] s_sizes; 
  private Heapfile f;
  private Scan scan;
  private Tuple     tuple1;
  private Tuple    Jtuple;
  private int        t1_size;
  private int nOutFlds;
  private CondExpr[]  OutputFilter;
  public FldSpec[] perm_mat;

 

  /**
   *constructor
   *@param file_name heapfile to be opened
   *@param in1[]  array showing what the attributes of the input fields are. 
   *@param s1_sizes[]  shows the length of the string fields.
   *@param len_in1  number of attributes in the input tuple
   *@param n_out_flds  number of fields in the out tuple
   *@param proj_list  shows what input fields go where in the output tuple
   *@param outFilter  select expressions
   *@exception IOException some I/O fault
   *@exception FileScanException exception from this class
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */
  public  FileScan (String  file_name,
		    AttrType in1[],                
		    short s1_sizes[], 
		    short     len_in1,              
		    int n_out_flds,
		    FldSpec[] proj_list,
		    CondExpr[]  outFilter        		    
		    )
    throws IOException,
	   FileScanException,
	   TupleUtilsException, 
	   InvalidRelation
    {
      _in1 = in1; 
      in1_len = len_in1;
      s_sizes = s1_sizes;
      
      Jtuple =  new Tuple();
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    ts_size;
      ts_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);
      
      OutputFilter = outFilter;
      perm_mat = proj_list;
      nOutFlds = n_out_flds; 
      tuple1 =  new Tuple();

      try {
	tuple1.setHdr(in1_len, _in1, s1_sizes);
      }catch (Exception e){
	throw new FileScanException(e, "setHdr() failed");
      }
      t1_size = tuple1.size();
      
      try {
	f = new Heapfile(file_name);
	
      }
      catch(Exception e) {
	throw new FileScanException(e, "Create new heapfile failed");
      }
      
      try {
	scan = f.openScan();
      }
      catch(Exception e){
	throw new FileScanException(e, "openScan() failed");
      }
    }
  
  /**
   *@return shows what input fields go where in the output tuple
   */
  public FldSpec[] show()
    {
      return perm_mat;
    }
  
  /**
   *@return the result tuple
   *@exception JoinsException some join exception
   *@exception IOException I/O errors
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception PredEvalException exception from PredEval class
   *@exception UnknowAttrType attribute type unknown
   *@exception FieldNumberOutOfBoundException array out of bounds
   *@exception WrongPermat exception for wrong FldSpec argument
   */
  public Tuple get_next()
    throws JoinsException,
       IOException,
       InvalidTupleSizeException,
       InvalidTypeException,
       PageNotReadException,
       PredEvalException,
       UnknowAttrType,
       FieldNumberOutOfBoundException,
       WrongPermat
    {
      RID rid = new RID();;

      while(true) {
        // *** DEBUG POINT 1: Is the underlying heap scan returning tuples? ***
        System.out.println("DEBUG: FileScan.get_next() - Calling heap scan getNext..."); // ADD THIS
        if((tuple1 =  scan.getNext(rid)) == null) {
          System.out.println("DEBUG: FileScan.get_next() - Heap scan returned null. End of scan."); // ADD THIS
          return null; // End of scan
        }
        System.out.println("DEBUG: FileScan.get_next() - Heap scan returned a tuple."); // ADD THIS

        // *** DEBUG POINT 2: Is the header being set correctly? ***
        // This is crucial for PredEval to read fields correctly.
        try {
            tuple1.setHdr(in1_len, _in1, s_sizes);
            System.out.println("DEBUG: FileScan.get_next() - Set header on tuple."); // ADD THIS
        } catch (Exception e) {
            System.err.println("ERROR: FileScan.get_next() - Exception setting header: " + e.getMessage()); // ADD THIS
            e.printStackTrace();
            // Decide how to handle header error - maybe continue to next tuple?
            continue; // Skip this tuple if header fails
        }

        // *** DEBUG POINT 3: Is PredEval being called and what does it return? ***
        boolean evalResult = false; // Default to false
        if (OutputFilter != null) { // Check if there's actually a filter
            try {
                System.out.println("DEBUG: FileScan.get_next() - Calling PredEval.Eval..."); // ADD THIS
                evalResult = PredEval.Eval(OutputFilter, tuple1, null, _in1, null);
                System.out.println("DEBUG: FileScan.get_next() - PredEval.Eval result: " + evalResult); // ADD THIS
            } catch (Exception e) {
                System.err.println("ERROR: FileScan.get_next() - Exception during PredEval: " + e.getMessage()); // ADD THIS
                e.printStackTrace();
                // Continue to next tuple if evaluation fails for some reason
                continue;
            }
        } else {
            evalResult = true; // No filter means the tuple passes
            System.out.println("DEBUG: FileScan.get_next() - No OutputFilter, evalResult defaults to true."); // ADD THIS
        }

        if (evalResult == true){ // Check the result of PredEval
          // *** DEBUG POINT 4: Is projection happening? ***
          try {
              System.out.println("DEBUG: FileScan.get_next() - Predicate passed. Calling Projection.Project..."); // ADD THIS
              Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds);
              System.out.println("DEBUG: FileScan.get_next() - Projection successful. Returning tuple."); // ADD THIS
              return  Jtuple; // Return the projected tuple
          } catch (Exception e) {
              System.err.println("ERROR: FileScan.get_next() - Exception during Projection: " + e.getMessage()); // ADD THIS
              e.printStackTrace();
              // Continue loop to get next tuple if projection fails
              continue;
          }
        } else {
             System.out.println("DEBUG: FileScan.get_next() - Predicate failed. Continuing loop."); // ADD THIS
            // Predicate failed, loop continues automatically
        }
      } // End while(true)
    }

  /**
   *implement the abstract method close() from super class Iterator
   *to finish cleaning up
   */
  public void close() 
    {
     
      if (!closeFlag) {
	scan.closescan();
	closeFlag = true;
      } 
    }
  
}


