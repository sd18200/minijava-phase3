//------------------------------------
// RelCatalog.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

import java.io.*;
import bufmgr.*;
import global.*;
import heap.*;
import diskmgr.*;
import index.*;
public class RelCatalog extends Heapfile
  implements  GlobalConst, Catalogglobal
{
  // Helps runStats
  //Status genStats(RelDesc &relRec, AttrDesc *&attrRecs);
  
  // CONSTRUCTOR
  RelCatalog(String filename)
    throws IOException, 
	   BufMgrException,
	   DiskMgrException,
	   Exception
    {
        super(filename);
      
      tuple = new Tuple(Tuple.max_size);
      
      attrs = new AttrType[5];
      attrs[0] = new AttrType(AttrType.attrString);
      attrs[1] = new AttrType(AttrType.attrInteger);
      attrs[2] = new AttrType(AttrType.attrInteger);
      attrs[3] = new AttrType(AttrType.attrInteger);
      attrs[4] = new AttrType(AttrType.attrInteger);
      
      str_sizes = new short[5];
      str_sizes[0] = (short)MAXNAME;
      
      try {
	tuple.setHdr((short)5, attrs, str_sizes);
      }
      catch (Exception e) {
	System.err.println ("tuple.setHdr"+e);
	throw new RelCatalogException(e, "setHdr() failed");
      }
    };
  
  
  // GET RELATION DESCRIPTION FOR A RELATION
  public void getInfo(String relation, RelDesc record)
    throws Catalogmissparam,
       Catalogioerror,
       Cataloghferror,
       RelCatalogException,
       IOException,
       Catalogrelnotfound
    {
      // int recSize; // Unused variable
      // RID rid = null; // Old incorrect line
      RID rid = new RID(); // *** FIX 1: Initialize the RID object ***
      Scan pscan = null;
      boolean found = false; // Flag to track if relation is found

      if (relation == null)
        throw new Catalogmissparam(null, "MISSING_PARAM relation");
      // Add null check for record for robustness
      if (record == null)
        throw new Catalogmissparam(null, "MISSING_PARAM record");


      try {
        pscan = new Scan(this);
      }
      catch (Exception e1) {
        System.err.println ("Scan initialization failed: "+e1); // Improved error message
        throw new RelCatalogException(e1, "scan failed");
      }

      try { // Use try-finally to ensure scan is closed
          while (true) {
            try {
              // Use the class member tuple, assuming it's okay for this context
              tuple = pscan.getNext(rid); // Pass the initialized RID
              if (tuple == null) {
                // *** FIX 2: Break on end of scan, don't throw here ***
                break; // Exit the loop if scan is finished
              }

              // *** FIX 3: Set the tuple header before reading fields ***
              try {
                  // Use the schema defined in the RelCatalog constructor
                  tuple.setHdr((short)5, attrs, str_sizes);
              } catch (Exception e_hdr) {
                  // Handle potential errors from setHdr (e.g., InvalidTypeException)
                  System.err.println("Failed to set tuple header in getInfo: " + e_hdr);
                  throw new RelCatalogException(e_hdr, "setHdr failed within getInfo loop");
              }
              // *** END FIX 3 ***


              // Now it's safe to read the fields
              read_tuple(tuple, record);

            } catch (Exception e_loop) { // Catch exceptions from getNext/setHdr/read_tuple
              System.err.println ("Error in getInfo loop (getNext/setHdr/read_tuple): "+e_loop);
              // Consider printing the stack trace of e_loop for more detail
              // e_loop.printStackTrace();
              throw new RelCatalogException(e_loop, "Error in getInfo loop (getNext/setHdr/read_tuple)");
            }

            // Check if the current record matches the requested relation name
            if (record.relName.equalsIgnoreCase(relation) == true) {
              found = true; // Mark as found
              break; // Exit the loop, we found the record
            }
          } // End while

          // After the loop, if 'found' is still false, THEN throw the exception
          if (!found) {
              throw new Catalogrelnotfound(null, "Catalog: Relation '" + relation + "' not Found in relcat!");
          }
          // If found, the 'record' object is already populated, so just return.
          return;

      } finally {
          // *** FIX 4: Ensure scan is always closed ***
          if (pscan != null) {
              pscan.closescan();
          }
      }
    };
  
  // CREATE A NEW RELATION
  public void createRel(String relation, int attrCnt, attrInfo [] attrList)
    throws Catalogmissparam,
       Catalogrelexists,
       Catalogdupattrs,
       Catalognomem,
       IOException,
       RelCatalogException,
       Catalogioerror,
       Cataloghferror,
       AttrCatalogException,
       Catalogbadtype // Added for AttrCatalog.addInfo
    {
      Heapfile rel; // For the user relation heapfile
      // RelDesc rd = null; // Old incorrect line
      RelDesc rd = new RelDesc(); // *** FIX 5: Initialize RelDesc object ***
      // AttrDesc ad = null; // Old incorrect line
      AttrDesc ad = new AttrDesc(); // *** FIX 6: Initialize AttrDesc object ***
      int tupleWidth = 0;
      int offset = 0;
      int sizeOfInt = 4;
      int sizeOfFloat = 4;
      int j;
      boolean relationExists = true; // Assume it exists initially

      if (relation== null  || attrCnt < 1)
        throw new Catalogmissparam(null, "MISSING_PARAM relation or attrCnt");
      if (attrList == null) // Added check for attrList
        throw new Catalogmissparam(null, "MISSING_PARAM attrList");


      // Check if relation already exists
      try {
        getInfo(relation, rd); // Pass the initialized rd
        // If getInfo returns without exception, relation exists
      }
      catch (Catalogrelnotfound e_notfound) {
        // This is the expected case for createRel - relation doesn't exist
        relationExists = false;
      }
      catch (Exception e_getinfo) { // Catch other potential errors from getInfo
        System.err.println("Unexpected error during getInfo in createRel: " + e_getinfo);
        // e_getinfo.printStackTrace(); // Optional: print stack trace
        throw new RelCatalogException(e_getinfo, "Failed during existence check in createRel");
      }


      if (relationExists == true)
        throw new Catalogrelexists(null, "Relation '" + relation + "' already exists!");

      // MAKE SURE THERE ARE NO DUPLICATE ATTRIBUTE NAMES and calculate offset/width
      offset = 0; // Start offset at 0
      for(int i = 0; i < attrCnt; i++) {
          // Check for null attribute name
          if (attrList[i] == null || attrList[i].attrName == null) {
              throw new Catalogmissparam(null, "Attribute definition or name is null at index " + i);
          }

          // Calculate length based on type
          int currentAttrLen = 0;
          switch (attrList[i].attrType.attrType) {
              case AttrType.attrString:
                  currentAttrLen = attrList[i].attrLen; // Use defined length
                  break;
              case AttrType.attrInteger:
                  currentAttrLen = sizeOfInt;
                  break;
              case AttrType.attrReal:
                  currentAttrLen = sizeOfFloat;
                  break;
              case AttrType.attrVector100D: // Assuming attrVector100D exists
                  currentAttrLen = attrList[i].attrLen; // Use defined length (e.g., 400)
                  break;
              default:
                  throw new Catalogbadtype(null, "Invalid attribute type for " + attrList[i].attrName);
          }
          // tupleWidth += currentAttrLen; // tupleWidth calculation wasn't actually used

          /* Duplicate attributes check.*/
          for(j = 0; j < i; j++) {
              if (attrList[j] != null && attrList[j].attrName != null &&
                  attrList[i].attrName.equalsIgnoreCase(attrList[j].attrName)) {
                  throw new Catalogdupattrs(null, "Duplicate attribute name: " + attrList[i].attrName);
              }
          }
          // Update offset for the *next* attribute
          // offset += currentAttrLen; // Offset calculation was slightly off, handled below
      }

      // Populate the initialized rd object for the relation catalog
      rd.relName = new String(relation);
      rd.attrCnt = attrCnt;
      rd.indexCnt = 0;
      rd.numTuples = 0; // Start with 0 tuples
      rd.numPages = 0; // Start with 0 pages (Heapfile constructor handles initial page)


      try {
        addInfo(rd); // Add relation entry to relcat heapfile
      }
      catch (Exception e_addRel) {
        System.err.println ("addInfo failed for RelDesc: "+e_addRel);
        // e_addRel.printStackTrace();
        throw new RelCatalogException(e_addRel, "addInfo failed for RelDesc");
      }

      // Populate and add attribute catalog entries
      ad.relName = new String(relation); // Set relation name once outside loop
      offset = 0; // Reset offset for attribute entries

      for (int i=0; i< attrCnt; i++) {
        // Populate the initialized ad object for each attribute
        ad.attrName = new String(attrList[i].attrName);
        ad.attrOffset = offset; // Current attribute's offset
        ad.attrType = new AttrType(attrList[i].attrType.attrType);
        ad.indexCnt = 0;
        ad.attrPos = i + 1;   // field position in the record (1-based)

        // *** FIX 7: Initialize minVal/maxVal before setting fields ***
        ad.minVal = new attrData();
        ad.maxVal = new attrData();

        int currentAttrLen = 0; // Calculate length again for offset update and ad.attrLen
        switch(attrList[i].attrType.attrType) {
            case AttrType.attrString:
                currentAttrLen = attrList[i].attrLen;
                ad.attrLen = currentAttrLen;
                // Use more appropriate defaults or leave null if not tracked
                ad.maxVal.strVal = ""; // Default empty string
                ad.minVal.strVal = ""; // Default empty string
                break;
            case AttrType.attrInteger:
                currentAttrLen = sizeOfInt;
                ad.attrLen = currentAttrLen;
                ad.minVal.intVal = Integer.MIN_VALUE; // Use standard min/max
                ad.maxVal.intVal = Integer.MAX_VALUE;
                break;
            case AttrType.attrReal:
                currentAttrLen = sizeOfFloat;
                ad.attrLen = currentAttrLen;
                ad.minVal.floatVal = -Float.MAX_VALUE; // Use standard min/max (note negative for min)
                ad.maxVal.floatVal = Float.MAX_VALUE;
                break;
            case AttrType.attrVector100D: // Assuming attrVector100D
                currentAttrLen = attrList[i].attrLen;
                ad.attrLen = currentAttrLen;
                // Min/Max for vectors might not be meaningful or easy to represent
                // Leave as default null/0 or handle appropriately if needed
                break;
            // No default needed if types are validated earlier or here
        }


        try {
          // Assuming ExtendedSystemDefs provides the correct AttrCatalog instance
          ExtendedSystemDefs.MINIBASE_ATTRCAT.addInfo(ad); // Add attribute entry to attrcat
        }
        catch (AttrCatalogException e_attr) { // Catch specific exception
          System.err.println ("AttrCatalog.addInfo failed: "+e_attr);
          // e_attr.printStackTrace();
          // Consider rolling back the relcat entry if attrcat fails? (More complex)
          throw e_attr;
        }
        catch (Exception e_addAttr) { // Catch other potential errors
          System.err.println ("AttrCatalog.addInfo failed with generic exception: "+e_addAttr);
          // e_addAttr.printStackTrace();
          throw new RelCatalogException(e_addAttr, "AttrCatalog.addInfo() failed");
        }

        offset += currentAttrLen; // Update offset for the next attribute
      }

      // NOW CREATE HEAPFILE for the user relation data
      try {
        rel = new Heapfile(relation); // Create the actual data heapfile
        if (rel == null) // Check if constructor returned null (unlikely but possible)
          throw new Catalognomem(null, "Heapfile creation returned null for " + relation);
        System.out.println("DEBUG: User data Heapfile created successfully for: " + relation); // Added debug
      }
      catch (Exception e_heap) {
        System.err.println ("Heapfile creation failed for " + relation + ": "+e_heap);
        // e_heap.printStackTrace();
        // Attempt to clean up catalog entries if heapfile creation fails? (More complex)
        throw new RelCatalogException(e_heap, "create user data heapfile failed");
      }

    };
  
  // ADD AN INDEX TO A RELATION
  // public void addIndex(String relation, String attrName,
	// 	       IndexType accessType, int buckets)
  //   throws RelCatalogException,
	//    IOException,
	//    Catalogioerror, 
	//    Cataloghferror, 
	//    Catalogmissparam,
	//    java.lang.Exception, 
	//    Catalogindexnotfound, 
	//    Catalognomem,
	//    Catalogbadtype, 
	//    Catalogattrnotfound,
	//    Exception
	   
  //   {
  //     RelDesc rd = null;
      
  //     if ((relation == null)||(attrName == null))
	// throw new Catalogmissparam(null, "MISSING_PARAM");
      
  //     // GET RELATION DATA
  //     try {
	// getInfo(relation, rd);
  //     }
  //     catch (Catalogioerror e) {
	// System.err.println ("Catalog I/O Error!"+e);
	// throw new Catalogioerror(null, "");
  //     }
  //     catch (Cataloghferror e1) {
	// System.err.println ("Catalog Heapfile Error!"+e1);
	// throw new Cataloghferror(null, "");
  //     }
  //     catch (Catalogmissparam e2) {
	// System.err.println ("Catalog Missing Param Error!"+e2);
	// throw new Catalogmissparam(null, "");
  //     }
      
      
  //     // CREATE INDEX FILE
  //     try {
	// ExtendedSystemDefs.MINIBASE_INDCAT.addIndex(relation, attrName,accessType, 0);
  //     }
  //     catch (Exception e2) {
	// System.err.println ("addIndex"+e2);
	// throw new RelCatalogException(e2, "addIndex failed");
  //     }
      
  //     // MODIFY INDEXCNT IN RELCAT
  //     rd.indexCnt++;
      
  //     try {
	// removeInfo(relation);
	// addInfo(rd);
  //     }
  //     catch (Catalogmissparam e4) {
	// throw e4;
  //     }
  //     catch (Exception e2) {
	// throw new RelCatalogException(e2, "add/remove info failed");
  //     }
      
  //   };
  
  
  
  // ADD INFORMATION ON A RELATION TO  CATALOG
  public void addInfo(RelDesc record)
    throws RelCatalogException,
       IOException
    {
      // RID rid; // This was unused, can be removed or commented out
      Tuple tempTuple = new Tuple(Tuple.max_size); // *** FIX: Create a local Tuple ***

      // *** FIX: Need to set the header for the new local tuple ***
      // Use the same schema definition as the class member tuple
      try {
          tempTuple.setHdr((short)5, attrs, str_sizes);
      } catch (Exception e_hdr) {
          System.err.println("Failed to set header on tempTuple in addInfo: " + e_hdr);
          throw new RelCatalogException(e_hdr, "setHdr failed in addInfo");
      }
      // *** END FIX ***

      try {
        // *** FIX: Pass the local tuple to make_tuple ***
        make_tuple(tempTuple, record);
      }
      catch (Exception e4) {
        System.err.println ("make_tuple"+e4);
        throw new RelCatalogException(e4, "make_tuple failed");
      }

      try {
        // *** FIX: Insert the data from the local tuple ***
        insertRecord(tempTuple.getTupleByteArray());
      }
      catch (Exception e2) {
        System.err.println ("insertRecord"+e2);
        throw new RelCatalogException(e2, "insertRecord failed");
      }
    };
  
  // REMOVE INFORMATION ON A RELATION FROM CATALOG
  public void removeInfo(String relation)
    throws RelCatalogException,
       IOException,
       Catalogmissparam,
       // Catalogattrnotfound, // Changed exception type below
       Catalogrelnotfound // More appropriate exception if relation not found
    {
      // *** FIX 1: Initialize RID ***
      RID rid = new RID();
      Scan pscan = null;
      // int recSize; // Unused
      // *** FIX 2: Initialize RelDesc ***
      RelDesc record = new RelDesc();
      boolean found = false; // Flag to track if relation is found

      if (relation == null)
    throw new Catalogmissparam(null, "MISSING_PARAM relation");

      try { // Use try-finally to ensure scan is closed
          try {
            pscan = new Scan(this);
          }
          catch (Exception e1) {
            System.err.println ("Scan initialization failed: "+e1); // Improved error message
            throw new RelCatalogException(e1, "scan failed");
          }

          while(true) {
            try {
              // Use the class member tuple (assuming okay for this context)
              tuple = pscan.getNext(rid); // Pass initialized RID

              // *** FIX 4: Correct end-of-scan handling ***
              if (tuple == null) {
                break; // End of scan, exit loop
              }

              // *** FIX 3: Set header before reading ***
              try {
                  // Use the schema defined in the RelCatalog constructor
                  tuple.setHdr((short)5, attrs, str_sizes);
              } catch (Exception e_hdr) {
                  System.err.println("Failed to set tuple header in removeInfo: " + e_hdr);
                  throw new RelCatalogException(e_hdr, "setHdr failed within removeInfo loop");
              }

              // Now read the tuple into the initialized record object
              read_tuple(tuple, record);

            }
            catch (Exception e_loop) { // Catch errors from getNext/setHdr/read_tuple
              System.err.println ("Error in removeInfo loop (getNext/setHdr/read_tuple): "+e_loop);
              // e_loop.printStackTrace(); // Optional: print stack trace
              throw new RelCatalogException(e_loop, "Error in removeInfo loop");
            }

            // Check if the current record matches
            if (record.relName.equalsIgnoreCase(relation)==true) {
              try {
                deleteRecord(rid); // Delete the found record
                found = true; // Mark as found
                break; // Exit loop after deleting
              }
              catch (Exception e_del) {
                System.err.println ("deleteRecord failed: "+e_del);
                throw new RelCatalogException(e_del, "deleteRecord failed");
              }
            }
          } // End while

          // *** FIX 4 (cont.): Throw exception only if not found after scan ***
          if (!found) {
               throw new Catalogrelnotfound(null, "Catalog: Relation '" + relation + "' not found for removal!");
          }
          // If found and deleted, just return normally
          return;

      } finally {
          // *** FIX 5: Ensure scan is always closed ***
          if (pscan != null) {
              pscan.closescan();
          }
      }
    };
  
  // Converts AttrDesc to tuple.
  public void make_tuple(Tuple tuple, RelDesc record)
    throws IOException, 
	   RelCatalogException
    {
      try {
   	tuple.setStrFld(1, record.relName);
	tuple.setIntFld(2, record.attrCnt);
	tuple.setIntFld(3, record.indexCnt);
	tuple.setIntFld(4, record.numTuples);
	tuple.setIntFld(5, record.numPages);
      }
      catch (Exception e1) {
	System.err.println ("setFld"+e1);
	throw new RelCatalogException(e1, "setFld failed");
      }
      
    };
  
  public void read_tuple(Tuple tuple, RelDesc record)
    throws IOException, 
	   RelCatalogException
    {
      try {
	record.relName = tuple.getStrFld(1);
	record.attrCnt = tuple.getIntFld(2);
	record.indexCnt = tuple.getIntFld(3);
	record.numTuples = tuple.getIntFld(4);
	record.numPages = tuple.getIntFld(5);
      }
      catch (Exception e1) {
	System.err.println ("getFld"+e1);
	throw new RelCatalogException(e1, "getFld failed");
      }
      
    };
  
  // Methods have not been implemented.
  
  // DESTROY A RELATION
  void destroyRel(String relation){};
  
  // DROP AN INDEX FROM A RELATION
  void dropIndex(String relation, String attrname, 
		 IndexType accessType){};
  
  // DUMPS A CATALOG TO A DISK FILE (FOR OPTIMIZER)
  void dumpCatalog(String filename){};
  
  // Collects stats from all the tables of the database.
  void runStats(String filename){};
  
  // OUTPUTS A RELATION TO DISK FOR OPTIMIZER
  // void dumpRelation(fstream outFile, RelDesc relRec, int tupleSize){}; 
  
  // OUTPUTS ATTRIBUTES TO DISK FOR OPTIMIZER (used by runstats)
  // void rsdumpRelAttributes (fstream outFile,AttrDesc [] attrRecs,
  //        int attrCnt, String relName){};
  
  // OUTPUTS ATTRIBUTES TO DISK FOR OPTIMIZER
  // void dumpRelAttributes (fstream outFile, AttrDesc [] attrRecs,
  //        int attrCnt){};
  
  // OUTPUTS ACCESS METHODS TO DISK FOR OPTIMIZER
  // void dumpRelIndex(fstream outFile,IndexDesc [] indexRecs,
  //                    int indexCnt, int attrsize){};
  
  
  Tuple tuple;
  short [] str_sizes;
  AttrType [] attrs;
  
};

