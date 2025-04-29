//------------------------------------
// AttrCatalog.java
//
// Ning Wang, April,24,  1998
//-------------------------------------

package catalog;

import java.io.*;
import global.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;


public class AttrCatalog extends Heapfile
	implements GlobalConst, Catalogglobal
{
  //OPEN ATTRIBUTE CATALOG
  AttrCatalog(String filename)
    throws java.io.IOException, 
	   HFException,
	   HFDiskMgrException,
	   HFBufMgrException,
	   AttrCatalogException
    {
      super(filename);
      
      int sizeOfInt = 4;
      int sizeOfFloat = 4;
      tuple = new Tuple(Tuple.max_size);
      attrs = new AttrType[9];
      
      attrs[0] = new AttrType(AttrType.attrString);
      attrs[1] = new AttrType(AttrType.attrString);
      attrs[2] = new AttrType(AttrType.attrInteger);
      attrs[3] = new AttrType(AttrType.attrInteger);
      attrs[4] = new AttrType(AttrType.attrInteger);  
      // AttrType will be represented by an integer
      // 0 = string, 1 = real, 2 = integer
      attrs[5] = new AttrType(AttrType.attrInteger);
      attrs[6] = new AttrType(AttrType.attrInteger);
      attrs[7] = new AttrType(AttrType.attrString);   // ?????  BK ?????
      attrs[8] = new AttrType(AttrType.attrString);   // ?????  BK ?????
      
      
      // Find the largest possible tuple for values attrs[7] & attrs[8]
      //   str_sizes[2] & str_sizes[3]
      max = 10;   // comes from attrData char strVal[10]
      if (sizeOfInt > max)
	max = (short) sizeOfInt;
      if (sizeOfFloat > max)
	max = (short) sizeOfFloat;
      
      
      str_sizes = new short[4];
      str_sizes[0] = (short) MAXNAME;
      str_sizes[1] = (short) MAXNAME;
      str_sizes[2] = max;
      str_sizes[3] = max;
      
      try {
	tuple.setHdr((short)9, attrs, str_sizes);
      }
      catch (Exception e) {
	throw new AttrCatalogException(e, "setHdr() failed");
      }
    };
  
  // GET ATTRIBUTE DESCRIPTION
  public void getInfo(String relation, String attrName, AttrDesc record)
    throws Catalogmissparam, 
	   Catalogioerror, 
	   Cataloghferror,
	   AttrCatalogException, 
	   IOException, 
	   Catalogattrnotfound
    {
      int recSize;
	  RID rid = new RID();
      Scan pscan = null; 
      
      
      if ((relation == null)||(attrName == null))
	throw new Catalogmissparam(null, "MISSING_PARAM");
      
      // OPEN SCAN
      
      try {
	pscan = new Scan(this);
      }
      catch (Exception e1) {
	throw new AttrCatalogException(e1, "scan failed");
      }
      
      // SCAN FILE FOR ATTRIBUTE
      // NOTE MUST RETURN ATTRNOTFOUND IF NOT FOUND!!!
      
      while (true){
        try {
          tuple = pscan.getNext(rid);
          if (tuple == null)
            throw new Catalogattrnotfound(null,"Catalog: Attribute not Found!");

          // *** ADD THIS LINE ***
          // Set the header using the schema defined in the constructor
          tuple.setHdr((short)9, attrs, str_sizes);
          // *** END ADDED LINE ***

          read_tuple(tuple, record);
        }
        catch (Exception e4) {
          // Consider adding more specific exception handling if needed
          // e4.printStackTrace(); // Optional logging
          throw new AttrCatalogException(e4, "read_tuple or setHdr failed in getInfo");
        }

        if ( record.relName.equalsIgnoreCase(relation)==true
             && record.attrName.equalsIgnoreCase(attrName)==true )
          {
            // Found the attribute, close scan and return
            pscan.closescan(); // Close scan before returning successfully
            return;
          }
      }
    };
  
  // GET ALL ATTRIBUTES OF A RELATION/
  // Return attrCnt
  public int getRelInfo(String relation, int attrCnt_unused_param, AttrDesc [] Attrs) // Renamed attrCnt param as it's not used effectively as input/output here
    throws Catalogmissparam,
       Catalogioerror,
       Cataloghferror,
       AttrCatalogException,
       IOException,
       Catalognomem,
       Catalogattrnotfound,
       Catalogindexnotfound, // Keep for now, though logic changes
       Catalogrelnotfound
    {
      // RelDesc record = null; // Old incorrect line
      RelDesc record = new RelDesc(); // *** FIX 1: Initialize RelDesc ***
      // AttrDesc attrRec = null; // Old incorrect line
      AttrDesc attrRec = new AttrDesc(); // *** FIX 2: Initialize AttrDesc for reading ***
      // int status; // Unused
      // int recSize; // Unused
      // RID rid = null; // Old incorrect line
      RID rid = new RID(); // *** FIX 3: Initialize RID ***
      Scan pscan = null;
      int count = 0;
      int actualAttrCnt = 0; // To store the count from relcat

      if (relation == null)
    throw new Catalogmissparam(null, "MISSING_PARAM relation");
      // *** FIX 4: Check the passed-in Attrs array ***
      if (Attrs == null)
        throw new Catalogmissparam(null, "MISSING_PARAM Attrs array (output)");


      // --- Get relation info from RelCatalog first ---
      try {
        // Pass the initialized record object
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relation, record);
        actualAttrCnt = record.attrCnt; // Get the expected attribute count
      }
      // ... Keep existing catch blocks for RelCatalog.getInfo errors ...
      catch (Catalogioerror e) {
    System.err.println ("Catalog I/O Error!"+e);
    throw new Catalogioerror(null, "");
      }
      catch (Cataloghferror e1) {
    System.err.println ("Catalog Heapfile Error!"+e1);
    throw new Cataloghferror(null, "");
      }
      catch (Catalogmissparam e2) { // Should not happen now for null record
    System.err.println ("Catalog Missing Param Error from RelCatalog.getInfo!"+e2);
    throw new Catalogmissparam(null, "");
      }
      catch (Catalogrelnotfound e3) {
    System.err.println ("Catalog: Relation not Found in relcat!"+e3);
    throw new Catalogrelnotfound(null, "");
      }
      catch (Exception e4) {
    e4.printStackTrace();
    throw new AttrCatalogException (e4, "RelCatalog.getInfo() failed within AttrCatalog.getRelInfo");
      }

      // Check if the caller provided a large enough array (optional but good practice)
      if (Attrs.length < actualAttrCnt) {
          // Or alternatively, throw an exception or just proceed carefully
          System.err.println("Warning: Attrs array provided might be smaller than actual attribute count (" + Attrs.length + " < " + actualAttrCnt + ")");
      }

      // If relation has no attributes according to relcat, return 0
      if (actualAttrCnt == 0)
    return 0;


      // --- Now scan AttrCatalog heapfile ---
      try { // Use try-finally to ensure scan is closed
          try {
            pscan = new Scan(this); // 'this' is the AttrCatalog heapfile
          }
          catch (Exception e1) {
            throw new AttrCatalogException(e1, "scan failed on attrcat");
          }

          // *** FIX 5: Remove reallocation of Attrs array ***
          // Attrs = new AttrDesc[actualAttrCnt]; // DO NOT REALLOCATE THE PARAMETER ARRAY

          count = 0; // Reset count for attributes found for this relation
		  count = 0; // Reset count for attributes found for this relation
          rid = new RID(); // Re-initialize RID for the scan (already done earlier, but safe)
          attrRec = new AttrDesc(); // Re-initialize AttrDesc for reading (already done earlier, but safe)
          attrRec.minVal = new attrData();
          attrRec.maxVal = new attrData();

          // SCAN FILE
          while(true) // Loop until break
            {
              try {
                tuple = pscan.getNext(rid); // Use initialized rid
                if (tuple == null) {
                    // *** FIX 8: End of scan reached, break the loop ***
                    break;
                }

                // *** FIX 6: Set tuple header before reading ***
                // Use the schema defined in the AttrCatalog constructor
                tuple.setHdr((short)9, attrs, str_sizes); // Assuming constructor setup is correct

                // Use initialized attrRec for reading
                read_tuple(tuple, attrRec);
              }
              catch (IOException e_io) { // Catch specific IO exceptions
                  throw new AttrCatalogException(e_io, "I/O error during attrcat scan");
              }
              catch (Exception e_scan) { // Catch other scan/read errors
                // Consider logging e_scan.printStackTrace()
                throw new AttrCatalogException(e_scan, "Error reading next record from attrcat");
              }

              // Check if the record belongs to the target relation
              if(attrRec.relName != null && attrRec.relName.equalsIgnoreCase(relation)==true)
                {
                  // Check bounds before assigning
                  if (attrRec.attrPos > 0 && (attrRec.attrPos - 1) < Attrs.length) {
                      // *** FIX 7: Deep copy the attribute info ***
                      Attrs[attrRec.attrPos - 1] = new AttrDesc(); // Create new object in array
                      // Manual deep copy:
                      Attrs[attrRec.attrPos - 1].relName = new String(attrRec.relName);
                      Attrs[attrRec.attrPos - 1].attrName = new String(attrRec.attrName);
                      Attrs[attrRec.attrPos - 1].attrOffset = attrRec.attrOffset;
                      Attrs[attrRec.attrPos - 1].attrType = new AttrType(attrRec.attrType.attrType);
                      Attrs[attrRec.attrPos - 1].attrLen = attrRec.attrLen;
                      Attrs[attrRec.attrPos - 1].indexCnt = attrRec.indexCnt;
                      Attrs[attrRec.attrPos - 1].attrPos = attrRec.attrPos;
                      // Assuming minVal/maxVal are not needed by createIndex, or copy them too if needed
                      // Attrs[attrRec.attrPos - 1].minVal = new attrData(attrRec.minVal); // Needs copy constructor/method
                      // Attrs[attrRec.attrPos - 1].maxVal = new attrData(attrRec.maxVal); // Needs copy constructor/method

                      count++; // Increment count of attributes found *for this relation*
                  } else {
                      System.err.println("Warning: Read attribute record with invalid attrPos (" + attrRec.attrPos + ") or index out of bounds for relation " + relation);
                  }
                }

              // No need to check count == actualAttrCnt here, scan the whole file
              // to ensure all attributes for the relation are found, regardless of order.
            } // End while

            // After scan, check if the correct number of attributes were found
            if (count != actualAttrCnt) {
                throw new Catalogattrnotfound(null, "Expected " + actualAttrCnt + " attributes for relation '" + relation + "' in attrcat, but found " + count);
            }

      } finally {
          // *** FIX 9: Ensure scan is closed ***
          if (pscan != null) {
              pscan.closescan();
          }
      }

      return actualAttrCnt; // Return the count found in relcat (which should match count found here)
    }
  
  // RETURNS ATTRTYPE AND STRINGSIZE ARRAYS FOR CONSTRUCTING TUPLES
  public int getTupleStructure(String relation, int attrCnt,
			       AttrType [] typeArray, short [] sizeArray)
    throws Catalogmissparam, 
	   Catalogioerror, 
	   Cataloghferror,
	   AttrCatalogException, 
	   IOException, 
	   Catalognomem, 
	   Catalogindexnotfound,
	   Catalogattrnotfound, 
	   Catalogrelnotfound
    {
      int  status;
      int stringcount = 0;
      AttrDesc [] attrs = null;
      int i, x;
      
      // GET ALL OF THE ATTRIBUTES
      
      try {
	attrCnt = getRelInfo(relation, attrCnt, attrs);
      }
      catch (Catalogioerror e) {
	System.err.println ("Catalog I/O Error!"+e);
	throw new Catalogioerror(null, "");
      }
      catch (Cataloghferror e1) {
	System.err.println ("Catalog Heapfile Error!"+e1);
	throw new Cataloghferror(null, "");
      }
      catch (Catalogmissparam e2) {
	System.err.println ("Catalog Missing Param Error!"+e2);
	throw new Catalogmissparam(null, "");
      }
      catch (Catalogindexnotfound e3) {
	System.err.println ("Catalog Index not Found!"+e3);
	throw new Catalogindexnotfound(null, "");
      }
      catch (Catalogattrnotfound e4) {
	System.err.println ("Catalog: Attribute not Found!"+e4);
	throw new Catalogattrnotfound(null, "");
      }
      catch (Catalogrelnotfound e5) {
	System.err.println ("Catalog: Relation not Found!"+e5);
	throw new Catalogrelnotfound(null, "");
      }
      
      
      // ALLOCATE TYPEARRAY
      
      typeArray = new AttrType[attrCnt];
      if (typeArray == null)
	throw new Catalognomem(null, "Catalog, No Enough Memory!");
      
      // LOCATE STRINGS
      
      for(i = 0; i < attrCnt; i++)
	{
	  if(attrs[i].attrType.attrType == AttrType.attrString)
	    stringcount++;
	}
      
      // ALLOCATE STRING SIZE ARRAY
      
      if(stringcount > 0) 
	{
	  sizeArray = new short[stringcount];
	  if (sizeArray == null)
	    throw new Catalognomem(null, "Catalog, No Enough Memory!");
	}
      
      // FILL ARRAYS WITH TYPE AND SIZE DATA
      
      for(x = 0, i = 0; i < attrCnt; i++)
	{
	  typeArray[i].attrType= attrs[i].attrType.attrType;
	  if(attrs[i].attrType.attrType == AttrType.attrString)
	    {
	      sizeArray[x] = (short) attrs[i].attrLen;
	      x++;
	    }
	}
      
      return attrCnt;    
    };
  
  
  // ADD ATTRIBUTE ENTRY TO CATALOG
  public void addInfo(AttrDesc record)
    throws AttrCatalogException, 
	   IOException
    {
      RID rid;
      
      try {
	make_tuple(tuple, record);
      }
      catch (Exception e4) {
	throw new AttrCatalogException(e4, "make_tuple failed");
      }
      
      try {
	insertRecord(tuple.getTupleByteArray());
      }
      catch (Exception e2) {
	throw new AttrCatalogException(e2, "insertRecord failed");
      }
    };
  
  
  // REMOVE AN ATTRIBUTE ENTRY FROM CATALOG
  // return true if success, false if not found.
  public void removeInfo(String relation, String attrName)
    throws AttrCatalogException,
           IOException,
           Catalogmissparam,
           Catalogattrnotfound

    {
      // int recSize; // Unused
      RID rid = new RID(); // <-- FIX 1: Initialize RID
      Scan pscan = null;
      AttrDesc record = new AttrDesc(); // <-- FIX 2: Initialize AttrDesc

      if ((relation == null)||(attrName == null))
        throw new Catalogmissparam(null, "MISSING_PARAM");

      try { // Use try-finally to ensure scan is closed
          // OPEN SCAN
          try {
            pscan = new Scan(this);
          }
          catch (Exception e1) {
            throw new AttrCatalogException(e1, "scan failed");
          }


          // SCAN FILE
          while (true) {
            try {
              tuple = pscan.getNext(rid); // <-- Use initialized rid
              if (tuple == null) {
                // If scan ends without finding, throw not found
                throw new Catalogattrnotfound(null, "Catalog: Attribute not Found!");
              }

              // <-- FIX 3: Add setHdr -->
              tuple.setHdr((short)9, attrs, str_sizes);

              read_tuple(tuple, record); // <-- Use initialized record
            }
            catch (Catalogattrnotfound e_nf) {
                throw e_nf; // Re-throw the specific exception if scan ended
            }
            catch (Exception e4) {
              // e4.printStackTrace(); // Optional logging
              throw new AttrCatalogException(e4, "getNext, setHdr, or read_tuple failed in removeInfo");
            }

            // Check if this is the record to delete
            if ( record.relName.equalsIgnoreCase(relation)==true
                 && record.attrName.equalsIgnoreCase(attrName)==true )
              {
                try {
                  deleteRecord(rid); // Use the rid obtained from getNext
                }
                catch (Exception e3) {
                  throw new AttrCatalogException(e3, "deleteRecord failed");
                }
                // Successfully deleted, exit loop (and method via finally block)
                return;
              }
          } // End while loop
      } finally {
          // Ensure scan is closed, regardless of how the try block exits
          if (pscan != null) {
              pscan.closescan();
          }
      }
    };
  
  
  //--------------------------------------------------
  // MAKE_TUPLE
  //--------------------------------------------------
  // Tuple must have been initialized properly in the 
  // constructor
  // Converts AttrDesc to tuple. 
  public void make_tuple(Tuple tuple, AttrDesc record)
    throws IOException,
       AttrCatalogException
    {
      try {
        tuple.setStrFld(1, record.relName);
        tuple.setStrFld(2, record.attrName);
        tuple.setIntFld(3, record.attrOffset);
        tuple.setIntFld(4, record.attrPos);

        // *** FIX: Handle Vector type explicitly ***
        if (record.attrType.attrType == AttrType.attrString) {
          tuple.setIntFld(5, 0); // Code for String
          tuple.setStrFld(8, record.minVal.strVal);
          tuple.setStrFld(9, record.maxVal.strVal);
        } else if (record.attrType.attrType == AttrType.attrReal) {
          tuple.setIntFld(5, 1); // Code for Real
          tuple.setFloFld(8,record.minVal.floatVal);
          // *** BUG FIX: Should be maxVal for field 9 ***
          tuple.setFloFld(9,record.maxVal.floatVal);
        } else if (record.attrType.attrType == AttrType.attrInteger) {
          tuple.setIntFld(5, 2); // Code for Integer
          tuple.setIntFld(8,record.minVal.intVal);
          tuple.setIntFld(9,record.maxVal.intVal);
        } else if (record.attrType.attrType == AttrType.attrVector100D) { // Assuming attrVector100D exists
          tuple.setIntFld(5, 3); // Code for Vector (Use 3 or another unused code)
          // Min/Max for vectors might not be stored or meaningful here
          // Set placeholder or handle appropriately if needed
          // Example: Store 0 or a specific marker if not used
          tuple.setIntFld(8, 0); // Placeholder for minVal
          tuple.setIntFld(9, 0); // Placeholder for maxVal
        } else {
            // Handle unknown type? Throw exception?
            throw new AttrCatalogException(null, "Unknown attribute type in make_tuple: " + record.attrType.attrType);
        }
        // *** END FIX ***

        tuple.setIntFld(6, record.attrLen);
        tuple.setIntFld(7, record.indexCnt);
      }
      catch (Exception e1) {
        throw new AttrCatalogException(e1, "make_tuple failed");
      }
    };
  
  
  //--------------------------------------------------
  // READ_TUPLE
  //--------------------------------------------------
  
  public void read_tuple(Tuple tuple, AttrDesc record)
    throws IOException,
       AttrCatalogException
    {
      try {
        record.relName = tuple.getStrFld(1);
        record.attrName = tuple.getStrFld(2);
        record.attrOffset = tuple.getIntFld(3);
        record.attrPos = tuple.getIntFld(4);

        int temp;
        temp = tuple.getIntFld(5);

        // *** FIX: Handle Vector type explicitly ***
        if (temp == 0) { // String
          record.attrType = new AttrType(AttrType.attrString);
          // Ensure minVal/maxVal are initialized before assignment
          if (record.minVal == null) record.minVal = new attrData();
          if (record.maxVal == null) record.maxVal = new attrData();
          record.minVal.strVal = tuple.getStrFld(8);
          record.maxVal.strVal = tuple.getStrFld(9);
        } else if (temp == 1) { // Real
          record.attrType = new AttrType(AttrType.attrReal);
          if (record.minVal == null) record.minVal = new attrData();
          if (record.maxVal == null) record.maxVal = new attrData();
          record.minVal.floatVal = tuple.getFloFld(8);
          record.maxVal.floatVal = tuple.getFloFld(9);
        } else if (temp == 2) { // Integer
          record.attrType = new AttrType(AttrType.attrInteger);
          if (record.minVal == null) record.minVal = new attrData();
          if (record.maxVal == null) record.maxVal = new attrData();
          record.minVal.intVal = tuple.getIntFld(8);
          record.maxVal.intVal = tuple.getIntFld(9);
        } else if (temp == 3) { // Vector (Using 3 as the code)
          record.attrType = new AttrType(AttrType.attrVector100D); // Assuming attrVector100D exists
          // Read placeholder min/max if stored, or ignore if not meaningful
          if (record.minVal == null) record.minVal = new attrData();
          if (record.maxVal == null) record.maxVal = new attrData();
          // Example: record.minVal.intVal = tuple.getIntFld(8); // If stored as int placeholder
          // Example: record.maxVal.intVal = tuple.getIntFld(9); // If stored as int placeholder
        } else {
          // Handle unknown type code? Throw exception?
          throw new AttrCatalogException(null, "Unknown attribute type code in read_tuple: " + temp);
        }
        // *** END FIX ***

        record.attrLen = tuple.getIntFld(6);
        record.indexCnt = tuple.getIntFld(7);
      }
      catch (Exception e1) {
        throw new AttrCatalogException(e1, "read_tuple failed");
      }
    }
  
  // REMOVE ALL ATTRIBUTE ENTRIES FOR A RELATION
  public void dropRelation(String relation){};
  
  // ADD AN INDEX TO A RELATION
  public void addIndex(String relation, String attrname,
		       IndexType accessType){};
  
  
  Tuple tuple;
  short [] str_sizes;
  AttrType [] attrs;
  short max;
};
