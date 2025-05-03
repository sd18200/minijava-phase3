// filepath: c:\Users\soumy\Desktop\510\minijava-phase3\javaminibase\src\iterator\PredEval.java
package iterator;

import heap.*;
import global.*;
import java.io.*;

public class PredEval
{
  /**
   *predicate evaluate, according to the condition ConExpr, judge if
   *the two tuple can join. if so, return true, otherwise false
   *@return true or false
   *@param p[] single select condition array (interpreted as ANDed conditions)
   *@param t1 compared tuple1 (usually outer relation or the only tuple for selection)
   *@param t2 compared tuple2 (usually inner relation or null for selection)
   *@param in1[] the attribute type corespond to the t1
   *@param in2[] the attribute type corespond to the t2
   *@exception IOException  some I/O error
   *@exception UnknowAttrType don't know the attribute type
   *@exception InvalidTupleSizeException size of tuple not valid
   *@exception InvalidTypeException type of tuple not valid
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception PredEvalException exception from this method
   */
  public static boolean Eval(CondExpr p[], Tuple t1, Tuple t2, AttrType in1[],
                 AttrType in2[])
    throws IOException,
       UnknowAttrType,
       InvalidTupleSizeException,
       InvalidTypeException,
       FieldNumberOutOfBoundException,
       PredEvalException // Added PredEvalException
    {
      CondExpr temp_ptr;
      int       i = 0;
      Tuple    value =   new Tuple(); // Tuple to hold literal values

      if (p == null)
    {
      return true; // No conditions means pass
    }

      while (p[i] != null)
    {
      temp_ptr = p[i];
      // This version assumes p[] is an array of ANDed conditions.
      // If OR logic (temp_ptr.next) is needed, the structure needs adjustment.

      // --- Process a single condition expression (temp_ptr) ---
      try {
          AttrType actual_comparison_type = null; // Type of the attribute involved
          Tuple    attr_tuple = null; // The tuple containing the attribute
          int      attr_fld_no = 0;   // Field number of the attribute in its tuple
          Tuple    literal_tuple = value; // Use 'value' tuple for the literal
          int      literal_fld_no = 1; // Literal is always field 1 in 'value' tuple

          // Case 1: attr CMP literal
          if (temp_ptr.type1.attrType == AttrType.attrSymbol && temp_ptr.type2.attrType != AttrType.attrSymbol) {
              attr_fld_no = temp_ptr.operand1.symbol.offset;
              // Determine which tuple (t1 or t2) holds the attribute
              if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer) {
                  attr_tuple = t1;
                  if (t1 == null) throw new PredEvalException("Outer tuple (t1) is null.");
                  if (in1 == null || attr_fld_no <= 0 || attr_fld_no > in1.length) throw new PredEvalException("Schema 'in1' missing or invalid for outer relation attribute " + attr_fld_no);
                  actual_comparison_type = in1[attr_fld_no - 1]; // Get type from schema
              } else { // Assuming inner relation if not outer
                  attr_tuple = t2;
                  // Allow t2 to be null for selection predicates (attr CMP literal)
                  if (t2 == null && in2 != null) { // Check if schema exists even if tuple is null
                     if (attr_fld_no <= 0 || attr_fld_no > in2.length) throw new PredEvalException("Schema 'in2' missing or invalid for inner relation attribute " + attr_fld_no);
                     actual_comparison_type = in2[attr_fld_no - 1];
                     // Cannot evaluate if inner tuple is null, this case shouldn't happen for selection
                     throw new PredEvalException("Inner tuple (t2) is null when evaluating attribute from inner relation.");
                  } else if (t2 == null) {
                     // This case is hit during FileScan where t2 is null.
                     // We rely on the outer relation check above.
                     // If operand1.symbol.relation was not outer, it's an error.
                     throw new PredEvalException("Invalid predicate: Attribute reference to inner relation when inner tuple (t2) is null.");
                  } else { // t2 is not null
                     if (in2 == null || attr_fld_no <= 0 || attr_fld_no > in2.length) throw new PredEvalException("Schema 'in2' missing or invalid for inner relation attribute " + attr_fld_no);
                     actual_comparison_type = in2[attr_fld_no - 1]; // Get type from schema
                  }
              }

              // Set up the literal_tuple ('value') based on operand2
              AttrType literal_type = temp_ptr.type2;


              short[] literal_str_size = null;
              if (literal_type.attrType == AttrType.attrString) {
                  literal_str_size = new short[] {(short)Math.min(temp_ptr.operand2.string.length()+1, Tuple.max_size)};
              } else if (literal_type.attrType == AttrType.attrVector100D) {
                  literal_str_size = null; // Or specific size if needed by setHdr
              }
              value.setHdr((short)1, new AttrType[]{literal_type}, literal_str_size);

              switch (literal_type.attrType) {
                  case AttrType.attrInteger: value.setIntFld(literal_fld_no, temp_ptr.operand2.integer); break;
                  case AttrType.attrReal:    value.setFloFld(literal_fld_no, temp_ptr.operand2.real);    break;
                  case AttrType.attrString:  value.setStrFld(literal_fld_no, temp_ptr.operand2.string);  break;
                  case AttrType.attrVector100D:
                      if (temp_ptr.operand2.vector == null) throw new PredEvalException("Literal vector is null in operand 2.");
                      value.setVectorFld(literal_fld_no, temp_ptr.operand2.vector.getValues());
                      break;
                  default: throw new UnknowAttrType("Unknown literal type in condition operand 2: " + literal_type.attrType);
              }

              // Perform comparison: attr_tuple vs literal_tuple
              int comp_res; // Declare comp_res

              // *** MODIFICATION START ***
              // Check for the special Real attribute vs. Integer literal case ('H' path)
              if (actual_comparison_type.attrType == AttrType.attrReal && literal_type.attrType == AttrType.attrInteger) {
                  try {
                      float t1_r = attr_tuple.getFloFld(attr_fld_no); // Get Real attribute
                      int t2_i = literal_tuple.getIntFld(literal_fld_no); // Get Int literal
                      int int_t1_r = (int) t1_r;                    // Cast attribute
                      if (int_t1_r == t2_i) comp_res = 0;
                      else if (int_t1_r < t2_i) comp_res = -1;
                      else comp_res = 1;
                      // Evaluate the result directly for this case
                      if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
                  } catch (Exception e) {
                      throw new PredEvalException (e, "Error during explicit Real vs Int comparison.");
                  }
              }
              else if (actual_comparison_type.attrType == AttrType.attrVector100D) {
                  if (literal_type.attrType != AttrType.attrVector100D) throw new PredEvalException("Cannot compare Vector attribute with non-Vector literal.");
                  int[] vec1 = attr_tuple.getVectorFld(attr_fld_no);
                  int[] vec2 = literal_tuple.getVectorFld(literal_fld_no);
                  double dist = TupleUtils.calculateEuclideanDistance(vec1, vec2);
                  if (!evaluateVectorOperator(dist, temp_ptr.op, temp_ptr.distance)) return false;
              }
              else {
                  comp_res = TupleUtils.CompareTupleWithValue(actual_comparison_type, attr_tuple, attr_fld_no, value); // Pass literal tuple 'value'
                  if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
              }
              // *** MODIFICATION END ***
          }
          // Case 2: literal CMP attr
          else if (temp_ptr.type1.attrType != AttrType.attrSymbol && temp_ptr.type2.attrType == AttrType.attrSymbol) {
              attr_fld_no = temp_ptr.operand2.symbol.offset;
              // Determine which tuple (t1 or t2) holds the attribute
              if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer) {
                  attr_tuple = t1;
                  if (t1 == null) throw new PredEvalException("Outer tuple (t1) is null.");
                  if (in1 == null || attr_fld_no <= 0 || attr_fld_no > in1.length) throw new PredEvalException("Schema 'in1' missing or invalid for outer relation attribute " + attr_fld_no);
                  actual_comparison_type = in1[attr_fld_no - 1]; // Get type from schema
              } else { // Assuming inner relation
                  attr_tuple = t2;
                   // Allow t2 to be null for selection predicates (literal CMP attr)
                  if (t2 == null && in2 != null) { // Check if schema exists even if tuple is null
                     if (attr_fld_no <= 0 || attr_fld_no > in2.length) throw new PredEvalException("Schema 'in2' missing or invalid for inner relation attribute " + attr_fld_no);
                     actual_comparison_type = in2[attr_fld_no - 1];
                     throw new PredEvalException("Inner tuple (t2) is null when evaluating attribute from inner relation.");
                  } else if (t2 == null) {
                     throw new PredEvalException("Invalid predicate: Attribute reference to inner relation when inner tuple (t2) is null.");
                  } else { // t2 is not null
                     if (in2 == null || attr_fld_no <= 0 || attr_fld_no > in2.length) throw new PredEvalException("Schema 'in2' missing or invalid for inner relation attribute " + attr_fld_no);
                     actual_comparison_type = in2[attr_fld_no - 1]; // Get type from schema
                  }
              }

              // Set up the literal_tuple ('value') based on operand1
              AttrType literal_type = temp_ptr.type1;
              short[] literal_str_size = null;
              if (literal_type.attrType == AttrType.attrString) {
                  literal_str_size = new short[] {(short)Math.min(temp_ptr.operand1.string.length()+1, Tuple.max_size)};
              } else if (literal_type.attrType == AttrType.attrVector100D) {
                  literal_str_size = null;
              }
              value.setHdr((short)1, new AttrType[]{literal_type}, literal_str_size);

              switch (literal_type.attrType) {
                  case AttrType.attrInteger: value.setIntFld(literal_fld_no, temp_ptr.operand1.integer); break;
                  case AttrType.attrReal:    value.setFloFld(literal_fld_no, temp_ptr.operand1.real);    break;
                  case AttrType.attrString:  value.setStrFld(literal_fld_no, temp_ptr.operand1.string);  break;
                  case AttrType.attrVector100D:
                      if (temp_ptr.operand1.vector == null) throw new PredEvalException("Literal vector is null in operand 1.");
                      value.setVectorFld(literal_fld_no, temp_ptr.operand1.vector.getValues());
                      break;
                  default: throw new UnknowAttrType("Unknown literal type in condition operand 1: " + literal_type.attrType);
              }

              // Perform comparison: literal_tuple vs attr_tuple
              int comp_res; // Declare comp_res

              // *** MODIFICATION START ***
              // Check for the special Real literal vs. Integer attribute case
              if (literal_type.attrType == AttrType.attrReal && actual_comparison_type.attrType == AttrType.attrInteger) {
                  try {
                      float t1_r = literal_tuple.getFloFld(literal_fld_no); // Get Real literal
                      int t2_i = attr_tuple.getIntFld(attr_fld_no);       // Get Int attribute
                      int int_t1_r = (int) t1_r;                          // Cast literal
                      if (int_t1_r == t2_i) comp_res = 0;
                      else if (int_t1_r < t2_i) comp_res = -1;
                      else comp_res = 1;
                      if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
                  } catch (Exception e) {
                      throw new PredEvalException (e, "Error during explicit Real literal vs Int attribute comparison.");
                  }
              }
              // Handle Vector comparison separately
              else if (actual_comparison_type.attrType == AttrType.attrVector100D) { // Check attribute type
                  if (literal_type.attrType != AttrType.attrVector100D) throw new PredEvalException("Cannot compare non-Vector literal with Vector attribute.");
                  int[] vec1 = literal_tuple.getVectorFld(literal_fld_no);
                  int[] vec2 = attr_tuple.getVectorFld(attr_fld_no);
                  double dist = TupleUtils.calculateEuclideanDistance(vec1, vec2);
                  if (!evaluateVectorOperator(dist, temp_ptr.op, temp_ptr.distance)) return false;
              }
              else {
                  // We need CompareTupleWithTuple directly here as CompareTupleWithValue assumes attr vs literal structure
                  comp_res = TupleUtils.CompareTupleWithTuple(literal_type, literal_tuple, literal_fld_no, attr_tuple, attr_fld_no);
                  if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
              }
              
          }
          // Case 3: attr CMP attr (Joins)
          else if (temp_ptr.type1.attrType == AttrType.attrSymbol && temp_ptr.type2.attrType == AttrType.attrSymbol) {
              int fld1_no = temp_ptr.operand1.symbol.offset;
              int fld2_no = temp_ptr.operand2.symbol.offset;
              Tuple tuple1_ref = null, tuple2_ref = null;
              AttrType type1 = null, type2 = null; // Actual types from schema

              // Assign tuples and types based on RelSpec
              if (temp_ptr.operand1.symbol.relation.key == RelSpec.outer) {
                  tuple1_ref = t1;
                  if (t1 == null) throw new PredEvalException("Outer tuple (t1) is null for join.");
                  if (in1 == null || fld1_no <= 0 || fld1_no > in1.length) throw new PredEvalException("Schema 'in1' missing or invalid for outer relation attribute " + fld1_no);
                  type1 = in1[fld1_no - 1];
              } else {
                  tuple1_ref = t2;
                  if (t2 == null) throw new PredEvalException("Inner tuple (t2) is null for join.");
                  if (in2 == null || fld1_no <= 0 || fld1_no > in2.length) throw new PredEvalException("Schema 'in2' missing or invalid for inner relation attribute " + fld1_no);
                  type1 = in2[fld1_no - 1];
              }

              if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer) {
                  tuple2_ref = t1;
                  if (t1 == null) throw new PredEvalException("Outer tuple (t1) is null for join (operand 2)."); // Added check
                  if (in1 == null || fld2_no <= 0 || fld2_no > in1.length) throw new PredEvalException("Schema 'in1' missing or invalid for outer relation attribute " + fld2_no);
                  type2 = in1[fld2_no - 1];
              } else {
                  tuple2_ref = t2;
                  if (t2 == null) throw new PredEvalException("Inner tuple (t2) is null for join (operand 2)."); // Added check
                  if (in2 == null || fld2_no <= 0 || fld2_no > in2.length) throw new PredEvalException("Schema 'in2' missing or invalid for inner relation attribute " + fld2_no);
                  type2 = in2[fld2_no - 1];
              }

              // Perform comparison
              int comp_res; 

             
              // Check for Real vs. Integer attribute join cases
              if (type1.attrType == AttrType.attrReal && type2.attrType == AttrType.attrInteger) {
                  try {
                      float t1_r = tuple1_ref.getFloFld(fld1_no); // Get Real attr1
                      int t2_i = tuple2_ref.getIntFld(fld2_no);   // Get Int attr2
                      int int_t1_r = (int) t1_r;                  // Cast attr1
                      if (int_t1_r == t2_i) comp_res = 0;
                      else if (int_t1_r < t2_i) comp_res = -1;
                      else comp_res = 1;
                      if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
                  } catch (Exception e) {
                      throw new PredEvalException (e, "Error during explicit Real vs Int attribute join comparison.");
                  }
              } else if (type1.attrType == AttrType.attrInteger && type2.attrType == AttrType.attrReal) {
                   try {
                      int t1_i = tuple1_ref.getIntFld(fld1_no);     // Get Int attr1
                      float t2_r = tuple2_ref.getFloFld(fld2_no);   // Get Real attr2
                      int int_t2_r = (int) t2_r;                    // Cast attr2
                      if (t1_i == int_t2_r) comp_res = 0;
                      else if (t1_i < int_t2_r) comp_res = -1;
                      else comp_res = 1;
                      if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
                  } catch (Exception e) {
                      throw new PredEvalException (e, "Error during explicit Int vs Real attribute join comparison.");
                  }
              }
              // Handle Vector comparison separately
              else if (type1.attrType == AttrType.attrVector100D) {
                  if (type2.attrType != AttrType.attrVector100D) throw new PredEvalException("Cannot compare Vector attribute with non-Vector attribute in join.");
                  int[] vec1 = tuple1_ref.getVectorFld(fld1_no);
                  int[] vec2 = tuple2_ref.getVectorFld(fld2_no);
                  double dist = TupleUtils.calculateEuclideanDistance(vec1, vec2);
                  if (!evaluateVectorOperator(dist, temp_ptr.op, temp_ptr.distance)) return false;
              }
              else {
                  if (type1.attrType != type2.attrType) {
                     System.err.println("Warning: Comparing attributes of incompatible types in join: " + type1.attrType + " vs " + type2.attrType + ". Relying on CompareTupleWithTuple handling.");
                  }
                  comp_res = TupleUtils.CompareTupleWithTuple(type1, tuple1_ref, fld1_no, tuple2_ref, fld2_no);
                  if (!evaluateOperator(comp_res, temp_ptr.op)) return false;
              }
          }
          // Case 4: literal CMP literal
          else {
              throw new PredEvalException("Comparison between two literals is not supported or invalid condition.");
          }

      } catch (FieldNumberOutOfBoundException e) {
          throw new PredEvalException(e, "Field number out of bounds: " + e.getMessage());
      } catch (IOException e) { // Catch IO exceptions from tuple access
          throw e; //  IO exceptions
      } catch (Exception e) { // Catch other potential errors during evaluation         
          System.err.println("ERROR in PredEval.Eval: " + e.getMessage()); // Log the error
          e.printStackTrace(); 
          throw new PredEvalException(e, "Error evaluating predicate: " + e.getMessage());
      }


      i++; // Move to next condition in the p[] array
    } // End while (p[i] != null)

      return true; // All conditions in p[] passed
    } // End Eval


    /**
     * Helper method to evaluate the comparison result against the operator for non-vector types.
     * @param comp_res Result from comparison (0, -1, or 1)
     * @param op The attribute operator
     * @return true if the condition holds, false otherwise
     */
    private static boolean evaluateOperator(int comp_res, AttrOperator op) {
        boolean result;
        switch (op.attrOperator) {
            case AttrOperator.aopEQ: result = (comp_res == 0); break;
            case AttrOperator.aopLT: result = (comp_res <  0); break;
            case AttrOperator.aopGT: result = (comp_res >  0); break;
            case AttrOperator.aopNE: result = (comp_res != 0); break;
            case AttrOperator.aopLE: result = (comp_res <= 0); break;
            case AttrOperator.aopGE: result = (comp_res >= 0); break;
            default:
                System.err.println("Warning: Unsupported or unknown operator (" + op.attrOperator + ") for non-vector comparison.");
                result = false; // Unknown or unsupported operator
        }
        return result;
    }

    /**
     * Helper method to evaluate the vector distance against the operator and threshold.
     * @param distance Calculated distance between vectors.
     * @param op The attribute operator (should include aopVECTORDIST).
     * @param threshold The distance threshold stored in the CondExpr (temp_ptr.distance).
     * @return true if the vector condition holds, false otherwise.
     */
    private static boolean evaluateVectorOperator(double distance, AttrOperator op, double threshold) {
         boolean result;
         switch (op.attrOperator) {
            // Standard operators applied to distance
            case AttrOperator.aopEQ: result = (Math.abs(distance - threshold) < 0.00001f); break; // Use tolerance
            case AttrOperator.aopLT: result = (distance <  threshold); break;
            case AttrOperator.aopGT: result = (distance >  threshold); break;
            case AttrOperator.aopNE: result = (Math.abs(distance - threshold) >= 0.00001f); break; // Use tolerance
            case AttrOperator.aopLE: result = (distance <= threshold); break;
            case AttrOperator.aopGE: result = (distance >= threshold); break;
            // Specific vector distance operator
            case AttrOperator.aopVECTORDIST: result = (distance <= threshold); break; // Common interpretation
            default:
                 System.err.println("Warning: Unsupported or unknown operator (" + op.attrOperator + ") for vector distance comparison.");
                 result = false; // Operator not applicable to vector distance
        }
        return result;
    }


} // End class PredEval