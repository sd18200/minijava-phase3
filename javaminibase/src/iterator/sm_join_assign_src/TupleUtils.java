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
public static int CompareTupleWithTuple(AttrType fldType,
                                        Tuple t1, int t1_fld_no,
                                        Tuple t2, int t2_fld_no)
    throws IOException, UnknowAttrType, TupleUtilsException {
    int t1_i, t2_i;
    float t1_r, t2_r;
    String t1_s, t2_s;

    switch (fldType.attrType) {
        case AttrType.attrInteger: // Compare two integers
            try {
                t1_i = t1.getIntFld(t1_fld_no);
                t2_i = t2.getIntFld(t2_fld_no);
            } catch (FieldNumberOutOfBoundException e) {
                throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
            }
            return Integer.compare(t1_i, t2_i);

        case AttrType.attrReal: // Compare two floats
            try {
                t1_r = t1.getFloFld(t1_fld_no);
                t2_r = t2.getFloFld(t2_fld_no);
            } catch (FieldNumberOutOfBoundException e) {
                throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
            }
            return Float.compare(t1_r, t2_r);

        case AttrType.attrString: // Compare two strings
            try {
                t1_s = t1.getStrFld(t1_fld_no);
                t2_s = t2.getStrFld(t2_fld_no);
            } catch (FieldNumberOutOfBoundException e) {
                throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
            }
            return t1_s.compareTo(t2_s);

        case AttrType.attrVector100D: // Handle attrVector100D
            try {
                int[] vector1 = t1.getVectorFld(t1_fld_no);
                int[] vector2 = t2.getVectorFld(t2_fld_no);
                return calculateDistance(vector1, vector2); // Return the distance
            } catch (FieldNumberOutOfBoundException e) {
                throw new TupleUtilsException(e, "FieldNumberOutOfBoundException is caught by TupleUtils.java");
            }

        default:
            throw new UnknowAttrType(null, "Don't know how to handle this attribute type");
    }
}


/**
 * Helper method to calculate the Euclidean distance between two 100D vectors.
 */
private static int calculateDistance(int[] vector1, int[] vector2) {
    int sum = 0;
    for (int i = 0; i < 100; i++) {
        int diff = vector1[i] - vector2[i];
        sum += diff * diff;
    }
    return (int) Math.sqrt(sum); // Return the Euclidean distance
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
      return CompareTupleWithTuple(fldType, t1, t1_fld_no, value, t1_fld_no);
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




