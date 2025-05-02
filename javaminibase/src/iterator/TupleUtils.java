package iterator;


import heap.*;
import global.*;
import java.io.*;
import java.lang.*;

/**
 *some useful method when processing Tuple 
 */
public class TupleUtils
{
  
  /**
   * This function compares a tuple with another tuple in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the tuple is greater,
   *   -1        if the tuple is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple.
   *@param    t2        another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.
   *@param    t2_fld_no the field numbers in the tuples to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,                              
   */
  public static int CompareTupleWithTuple(AttrType fldType, // This is the type of the *attribute* field
                                          Tuple    t1, int t1_fld_no, // Tuple containing the attribute
                                          Tuple    t2, int t2_fld_no) // Tuple containing the literal (or another attr)
    throws IOException, UnknowAttrType, TupleUtilsException
 {
      int   t1_i = 0, t2_i = 0; // Initialize locals
      float t1_r = 0.0f, t2_r = 0.0f;
      String t1_s = null, t2_s = null;

     // --- Type-guessing logic removed. ---
     // The caller (PredEval) is now responsible for handling the
     // specific Real attribute vs. Integer literal case directly.

     // --- Standard comparison logic based ONLY on fldType (attribute type) ---
     // Assumes t2 contains a value compatible with fldType for comparison.
     System.out.println("DEBUG: TupleUtils (Simplified) - Entering switch for type: " + fldType.attrType);
     switch (fldType.attrType) {
      case AttrType.attrInteger:
          try {
              t1_i = t1.getIntFld(t1_fld_no);
              // Assuming t2 contains an Integer literal if fldType is Integer
              t2_i = t2.getIntFld(t2_fld_no);
              System.out.println("DEBUG: TupleUtils (Switch - Int case): Comparing " + t1_i + " vs " + t2_i);
          } catch (Exception e) {
              throw new TupleUtilsException(e, "Type mismatch or error comparing integers. Field " + t1_fld_no + " vs " + t2_fld_no);
          }
          if (t1_i == t2_i) return 0;
          if (t1_i < t2_i) return -1;
          return 1;

      case AttrType.attrReal: // Handles Real vs Real comparison ('N' case or 'H' fallback)
          System.out.println("DEBUG: TupleUtils.CompareTupleWithTuple (Switch - Real case) - Entered Real vs Real.");
          try {
              // We know t1 is Real (fldType).
              // We assume t2 contains a Real literal because the Real vs Int case is handled earlier.
              t1_r = t1.getFloFld(t1_fld_no);
              t2_r = t2.getFloFld(t2_fld_no); // Assumes t2 holds a float

              System.out.println("DEBUG: TupleUtils.CompareTupleWithTuple (Switch - Real case): Comparing " + t1_r + " vs " + t2_r);
              final float EPSILON = 0.00001f;
              System.out.println("DEBUG: TupleUtils.CompareTupleWithTuple (Switch - Real case): Checking Math.abs(" + t1_r + " - " + t2_r + ") < " + EPSILON);
              if (Math.abs(t1_r - t2_r) < EPSILON) {
                  System.out.println("DEBUG: TupleUtils.CompareTupleWithTuple (Switch - Real case): Values are equal within epsilon.");
                  return 0; // Considered equal
              } else if (t1_r < t2_r) {
                  System.out.println("DEBUG: TupleUtils.CompareTupleWithTuple (Switch - Real case): t1 < t2.");
                  return -1; // t1 is smaller
              } else { // t1_r > t2_r
                  System.out.println("DEBUG: TupleUtils.CompareTupleWithTuple (Switch - Real case): t1 > t2.");
                  return 1; // t1 is greater
              }
          } catch (Exception e) {
              System.err.println("ERROR: TupleUtils.CompareTupleWithTuple (Switch - Real case) - Exception: " + e.getMessage());
              e.printStackTrace();
              throw new TupleUtilsException(e, "Type mismatch or error comparing floats. Field " + t1_fld_no + " vs " + t2_fld_no);
          }
         // break; // Unreachable after return

      case AttrType.attrString:
          try {
              t1_s = t1.getStrFld(t1_fld_no);
              t2_s = t2.getStrFld(t2_fld_no); // Assumes t2 holds a string
              System.out.println("DEBUG: TupleUtils (Switch - String case): Comparing \"" + t1_s + "\" vs \"" + t2_s + "\"");
          } catch (FieldNumberOutOfBoundException e){
              throw new TupleUtilsException(e, "FieldNumberOutOfBoundException comparing strings.");
          }
          int cmp = t1_s.compareTo(t2_s);
          if(cmp > 0) return 1;
          if (cmp < 0) return -1;
          return 0;
          // break; // Unreachable after return

      case AttrType.attrVector100D:
          try {
              int[] vector1 = t1.getVectorFld(t1_fld_no);
              int[] vector2 = t2.getVectorFld(t2_fld_no); // Assumes t2 holds a vector
              // Assuming comparison based on distance from origin
              int[] origin = new int[100]; // Assuming vector size is 100
              java.util.Arrays.fill(origin, 0);
              double distance1 = calculateEuclideanDistance(vector1, origin);
              double distance2 = calculateEuclideanDistance(vector2, origin);
              System.out.println("DEBUG: TupleUtils (Switch - Vector case): Comparing Dist1=" + distance1 + " vs Dist2=" + distance2);
              return Double.compare(distance1, distance2);
          } catch (FieldNumberOutOfBoundException e) {
              throw new TupleUtilsException(e, "FieldNumberOutOfBoundException comparing vectors.");
          } catch (Exception e) {
              throw new TupleUtilsException(e, "Error comparing vectors.");
          }
          // break; // Unreachable after return

      default:
          throw new UnknowAttrType(null, "Don't know how to handle comparison for attribute type " + fldType.attrType);

    } // End switch
 }
  
    public static double calculateEuclideanDistance(int[] vector1, int[] vector2) {
      double sum = 0;
      for (int i = 0; i < vector1.length; i++) {
          int diff = vector1[i] - vector2[i];
          sum += diff * diff;
      }
      return Math.sqrt(sum);
  }


  
  
  /**
   * This function  compares  tuple1 with another tuple2 whose
   * field number is same as the tuple1
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one tuple
   *@param    value     another tuple.
   *@param    t1_fld_no the field numbers in the tuples to be compared.  
   *@return   0        if the two are equal,
   *          1        if the tuple is greater,
   *         -1        if the tuple is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class   
   */            
  public static int CompareTupleWithValue(AttrType fldType,
					  Tuple  t1, int t1_fld_no,
					  Tuple  value)
    throws IOException,
	   UnknowAttrType,
	   TupleUtilsException
    {
      return CompareTupleWithTuple(fldType, t1, t1_fld_no, value, 1);
    }
  
  /**
   *This function Compares two Tuple inn all fields 
   * @param t1 the first tuple
   * @param t2 the secocnd tuple
   * @param type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */            
  
  public static boolean Equal(Tuple t1, Tuple t2, AttrType types[], int len)
    throws IOException,UnknowAttrType,TupleUtilsException
    {
      int i;
      
      for (i = 1; i <= len; i++)
	if (CompareTupleWithTuple(types[i-1], t1, i, t2, i) != 0)
	  return false;
      return true;
    }
  
  /**
   *get the string specified by the field number
   *@param tuple the tuple 
   *@param fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */
  public static String Value(Tuple  tuple, int fldno)
    throws IOException,
	   TupleUtilsException
    {
      String temp;
      try{
	temp = tuple.getStrFld(fldno);
      }catch (FieldNumberOutOfBoundException e){
	throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
      }
      return temp;
    }
  
 
  /**
   *set up a tuple in specified field from a tuple
   *@param value the tuple to be set 
   *@param tuple the given tuple
   *@param fld_no the field number
   *@param fldType the tuple attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */  
  public static void SetValue(Tuple value, Tuple  tuple, int fld_no, AttrType fldType)
    throws IOException,
	   UnknowAttrType,
	   TupleUtilsException
    {
      
      switch (fldType.attrType)
	{
	case AttrType.attrInteger:
	  try {
	    value.setIntFld(fld_no, tuple.getIntFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	case AttrType.attrReal:
	  try {
	    value.setFloFld(fld_no, tuple.getFloFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
	case AttrType.attrString:
	  try {
	    value.setStrFld(fld_no, tuple.getStrFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
	  }
	  break;
  case AttrType.attrVector100D:
    try {
      int[] vector = tuple.getVectorFld(fld_no);
      value.setVectorFld(fld_no, vector);
    } catch (FieldNumberOutOfBoundException e) {
      throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
    }
    break;

	default:
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
      
      return;
    }
  
  
  /**
   *set up the Jtuple's attrtype, string size,field number for using join
   *@param Jtuple  reference to an actual tuple  - no memory has been malloced
   *@param res_attrs  attributes type of result tuple
   *@param in1  array of the attributes of the tuple (ok)
   *@param len_in1  num of attributes of in1
   *@param in2  array of the attributes of the tuple (ok)
   *@param len_in2  num of attributes of in2
   *@param t1_str_sizes shows the length of the string fields in S
   *@param t2_str_sizes shows the length of the string fields in R
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   */
  public static short[] setup_op_tuple(Tuple Jtuple, AttrType[] res_attrs,
				       AttrType in1[], int len_in1, AttrType in2[], 
				       int len_in2, short t1_str_sizes[], 
				       short t2_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   TupleUtilsException
    {
      short [] sizesT1 = new short [len_in1];
      short [] sizesT2 = new short [len_in2];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesT1[i] = t1_str_sizes[count++];
      
      for (count = 0, i = 0; i < len_in2; i++)
	if (in2[i].attrType == AttrType.attrString)
	  sizesT2[i] = t2_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer)
	    res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  else if (proj_list[i].relation.key == RelSpec.innerRel)
	    res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
	}
      try {
	Jtuple.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new TupleUtilsException(e,"setHdr() failed");
      }
      return res_str_sizes;
    }
  
 
   /**
   *set up the Jtuple's attrtype, string size,field number for using project
   *@param Jtuple  reference to an actual tuple  - no memory has been malloced
   *@param res_attrs  attributes type of result tuple
   *@param in1  array of the attributes of the tuple (ok)
   *@param len_in1  num of attributes of in1
   *@param t1_str_sizes shows the length of the string fields in S
   *@param proj_list shows what input fields go where in the output tuple
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception TupleUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   */

  public static short[] setup_op_tuple(Tuple Jtuple, AttrType res_attrs[],
				       AttrType in1[], int len_in1,
				       short t1_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   TupleUtilsException, 
	   InvalidRelation
    {
      short [] sizesT1 = new short [len_in1];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesT1[i] = t1_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer) 
            res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  
	  else throw new InvalidRelation("Invalid relation -innerRel");
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer
	      && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
	    n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++) {
	if (proj_list[i].relation.key ==RelSpec.outer
	    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
	  res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
      }
     
      try {
	Jtuple.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new TupleUtilsException(e,"setHdr() failed");
      } 
      return res_str_sizes;
    }
}




