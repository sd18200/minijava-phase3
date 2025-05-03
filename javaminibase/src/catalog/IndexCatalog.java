//------------------------------------
// IndexCatalog.java
//
// Ning Wang, April,24, 1998
//-------------------------------------

package catalog;

import java.io.*;

import global.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;

public class IndexCatalog extends Heapfile
  implements GlobalConst, Catalogglobal
{
  private static final short NUM_FIELDS = 8;
  // OPEN INDEX CATALOG
  IndexCatalog(String filename)
    throws IOException,
       BufMgrException,
       DiskMgrException,
       Exception
         {
            // Call the Heapfile constructor
            super(filename); // This MUST be the first line



            try {
                // Initialize the tuple member variable
                tuple = new Tuple(Tuple.max_size);
                if (tuple == null) { // Check immediately after creation
                    System.err.println("CRITICAL ERROR: IndexCatalog constructor - tuple is NULL immediately after new Tuple()!");
                    throw new RuntimeException("Failed to allocate Tuple in IndexCatalog constructor"); // Fail fast
                } else {
                }

                // Initialize schema arrays
                attrs = new AttrType[NUM_FIELDS]; // Use NUM_FIELDS
                attrs[0] = new AttrType(AttrType.attrString); // relName
                attrs[1] = new AttrType(AttrType.attrString); // attrName
                attrs[2] = new AttrType(AttrType.attrInteger); // accessType
                attrs[3] = new AttrType(AttrType.attrInteger); // order
                attrs[4] = new AttrType(AttrType.attrInteger); // clustered
                attrs[5] = new AttrType(AttrType.attrInteger); // distinctKeys
                attrs[6] = new AttrType(AttrType.attrInteger); // indexPages
                attrs[7] = new AttrType(AttrType.attrString); // physicalFileName <-- ADDED

                str_sizes = new short[3]; // Increase size to 3
                str_sizes[0] = (short)MAXNAME; // relName
                str_sizes[1] = (short)MAXNAME; // attrName
                // *** IMPORTANT: Ensure MAXFILENAME is large enough in GlobalConst.java ***
                // If MAXFILENAME is only 15, this will likely truncate filenames.
                str_sizes[2] = (short)MAXFILENAME; // physicalFileName <-- ADDED

                // Set the header for the tuple
                tuple.setHdr(NUM_FIELDS, attrs, str_sizes); // Use NUM_FIELDS

                size = tuple.size(); // Set the size based on the header

            } catch (Exception e) {
                System.err.println("CRITICAL ERROR: Exception during IndexCatalog tuple/header initialization!");
                e.printStackTrace();
                // Rethrow or handle appropriately - this object might be unusable
                throw new RuntimeException("Failed to initialize IndexCatalog tuple/header", e);
            }

          }
  
  // GET ALL INDEXES FOR A RELATION
  // Return indexCnt.
  public int getRelInfo(String relation, int indexCnt_unused, IndexDesc [] indexes)
    throws Catalogmissparam, Catalogioerror, Cataloghferror, Catalogindexnotfound,
           IOException, Catalognomem, Catalogattrnotfound, IndexCatalogException,
           RelCatalogException, Catalogrelnotfound
    {
      RelDesc record = new RelDesc();
      RID rid = new RID();
      Scan pscan = null;
      int count = 0;
      int actualIndexCnt = 0;

       if (relation == null) throw new Catalogmissparam(null, "MISSING_PARAM relation");
       if (indexes == null) throw new Catalogmissparam(null, "MISSING_PARAM indexes array (output)");
       try {
         ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relation, record);
         actualIndexCnt = record.indexCnt;
       } catch (Exception e_rel) {
           throw new IndexCatalogException(e_rel, "RelCatalog.getInfo failed within IndexCatalog.getRelInfo");
       }
       if (actualIndexCnt == 0) return 0;
       if (indexes.length < actualIndexCnt) {
           throw new Catalognomem(null, "Provided indexes array is too small (" + indexes.length + " < " + actualIndexCnt + ")");
       }
      // --- Scan the IndexCatalog heapfile ---
      try {
          try {
            pscan = new Scan(this);
          } catch (Exception e) {
            throw new IndexCatalogException(e,"scan() failed");
          }

          count = 0;
          IndexDesc tempIndexRec = new IndexDesc();
          Tuple currentTuple = null; // Use a local Tuple for reading

          while(true)
            {
              try {
                currentTuple = pscan.getNext(rid);
                if (currentTuple == null) {
                  break; // End of scan
                }

                // Set header on the local tuple
                if (this.attrs == null || this.str_sizes == null) {
                    throw new IndexCatalogException(null, "Internal error: IndexCatalog schema (attrs/str_sizes) not initialized.");
                }
                // *** Use NUM_FIELDS ***
                currentTuple.setHdr(NUM_FIELDS, this.attrs, this.str_sizes);

                // Read from the local tuple into tempIndexRec
                read_tuple(currentTuple, tempIndexRec);
              }
              catch (Exception e4) {
                throw new IndexCatalogException(e4,"getNext, setHdr, or read_tuple() failed");
              }

              // Check if the found index belongs to the target relation
              if(tempIndexRec.relName != null && tempIndexRec.relName.equalsIgnoreCase(relation)==true)
                {
                  if (count < indexes.length) {
                      // Deep copy needed for the output array
                      indexes[count] = new IndexDesc();
                      indexes[count].relName = new String(tempIndexRec.relName);
                      indexes[count].attrName = new String(tempIndexRec.attrName);
                      if (tempIndexRec.accessType == null || tempIndexRec.order == null) {
                          throw new IndexCatalogException(null, "Inconsistent data: Null accessType or order found.");
                      }
                      indexes[count].accessType = new IndexType(tempIndexRec.accessType.indexType);
                      indexes[count].order = new TupleOrder(tempIndexRec.order.tupleOrder);
                      indexes[count].clustered = tempIndexRec.clustered;
                      indexes[count].distinctKeys = tempIndexRec.distinctKeys;
                      indexes[count].indexPages = tempIndexRec.indexPages;
                      // *** Deep copy physicalFileName ***
                      indexes[count].physicalFileName = (tempIndexRec.physicalFileName != null) ? new String(tempIndexRec.physicalFileName) : null;
                      count++;
                  } else {
                      System.err.println("Warning: Found more indexes for relation " + relation + " than expected (" + actualIndexCnt + ")");
                      break; // Avoid array out of bounds
                  }
                }
            } // End while

            if (count != actualIndexCnt) {
                // This might indicate an inconsistency between relcat and indexcat
                 System.err.println("Warning: Expected " + actualIndexCnt + " indexes for relation '" + relation + "' based on relcat, but found " + count + " in indexcat.");
                // Depending on requirements, you might throw Catalogindexnotfound or just return count
                // throw new Catalogindexnotfound(null, "Expected " + actualIndexCnt + " indexes for relation '" + relation + "' based on relcat, but found " + count + " in indexcat.");
            }

      } finally {
            if (pscan != null) {
                pscan.closescan();
            }
      }
      return count; // Return the actual number found and copied
    };
  



      // RETURN INFO ON AN INDEX
  public void getInfo(String relation, String attrName,
              IndexType accessType, IndexDesc record)
    throws Catalogmissparam, Catalogioerror, Cataloghferror, IOException,
           Catalogindexnotfound, IndexCatalogException
    {
      if (relation == null || attrName == null || accessType == null || record == null)
        throw new Catalogmissparam(null, "MISSING_PARAM in getInfo");

      RID rid = new RID();
      Scan pscan = null;
      boolean indexFound = false;

      try {
          try {
            pscan = new Scan(this);
          } catch (Exception e1) {
            throw new IndexCatalogException(e1, "Scan construction failed in getInfo");
          }

          Tuple currentTuple = null; // Use a local Tuple for reading

          while (true)
            {
              try {
                currentTuple = pscan.getNext(rid);
              } catch (Exception e_next) {
                  throw new IndexCatalogException(e_next, "pscan.getNext(rid) failed in getInfo");
              }

              if (currentTuple == null) {
                  break; // End of scan
              }

              try {
                if (this.attrs == null || this.str_sizes == null) {
                   throw new IndexCatalogException(null, "Internal Error: IndexCatalog schema (attrs/str_sizes) is null in getInfo!");
                }
                // *** Use NUM_FIELDS ***
                currentTuple.setHdr(NUM_FIELDS, this.attrs, this.str_sizes);
              } catch (Exception e_hdr) {
                  throw new IndexCatalogException(e_hdr, "tuple.setHdr() failed in getInfo");
              }

              try {
                // Initialize sub-objects if null before reading into them
                if (record.accessType == null) record.accessType = new IndexType(IndexType.None);
                if (record.order == null) record.order = new TupleOrder(TupleOrder.Ascending);
                read_tuple(currentTuple, record);
              } catch (Exception e_read) {
                  throw new IndexCatalogException(e_read, "read_tuple call failed in getInfo");
              }

              // Check if this record matches the requested index
              if(record.relName != null && record.attrName != null && record.accessType != null &&
                 record.relName.equalsIgnoreCase(relation)==true &&
                 record.attrName.equalsIgnoreCase(attrName)==true &&
                 (record.accessType.indexType == accessType.indexType))
                {
                  indexFound = true; // FOUND
                  break; // Exit the loop
                }
            } // End While

            if (!indexFound) {
                throw new Catalogindexnotfound(null,"Catalog: Index not Found for " + relation + "." + attrName + " with type " + accessType.indexType);
            }

      } finally {
          if (pscan != null) {
              pscan.closescan();
          }
      }
    };
  
  // GET ALL INDEXES INLUDING A SPECIFIED ATTRIBUTE
  public int getAttrIndexes(String relation,
                String attrName, int indexCnt_unused, IndexDesc [] indexes)
    throws Catalogmissparam, Catalogioerror, Cataloghferror, IOException,
           Catalognomem, Catalogindexnotfound, Catalogattrnotfound, IndexCatalogException
    {
      AttrDesc attrRec = new AttrDesc();
      RID rid = new RID();
      Scan pscan = null;
      int count = 0;
      int actualIndexCnt = 0;

       if (relation == null || attrName == null) throw new Catalogmissparam(null, "MISSING_PARAM relation or attrName");
       if (indexes == null) throw new Catalogmissparam(null, "MISSING_PARAM indexes array (output)");
       try {
         ExtendedSystemDefs.MINIBASE_ATTRCAT.getInfo(relation, attrName, attrRec);
         actualIndexCnt = attrRec.indexCnt;
       } catch (Exception e4) {
           throw new IndexCatalogException (e4,"AttrCatalog.getInfo() failed within getAttrIndexes");
       }
       if(actualIndexCnt == 0) return 0;
       if (indexes.length < actualIndexCnt) {
           throw new Catalognomem(null, "Provided indexes array is too small (" + indexes.length + " < " + actualIndexCnt + ")");
       }
      // --- Scan IndexCatalog ---
      try {
          try {
            pscan = new Scan(this);
          } catch (Exception e) {
            throw new IndexCatalogException(e,"scan failed");
          }

          count = 0;
          IndexDesc tempIndexRec = new IndexDesc();
          Tuple currentTuple = null; // Use a local Tuple for reading

          while(true)
            {
              try {
                currentTuple = pscan.getNext(rid);
                if (currentTuple == null) {
                  break; // End of scan
                }

                if (this.attrs == null || this.str_sizes == null) {
                    throw new IndexCatalogException(null, "Internal error: IndexCatalog schema not initialized.");
                }
                // *** Use NUM_FIELDS ***
                currentTuple.setHdr(NUM_FIELDS, this.attrs, this.str_sizes);

                read_tuple(currentTuple, tempIndexRec);
              }
              catch (Exception e4) {
                throw new IndexCatalogException(e4, "getNext, setHdr, or read_tuple() failed");
              }

              // Check if it matches the relation and attribute
              if(tempIndexRec.relName != null && tempIndexRec.attrName != null &&
                 tempIndexRec.relName.equalsIgnoreCase(relation)==true &&
                 tempIndexRec.attrName.equalsIgnoreCase(attrName)==true)
                {
                  if (count < indexes.length) {
                      // Deep copy needed
                      indexes[count] = new IndexDesc();
                      indexes[count].relName = new String(tempIndexRec.relName);
                      indexes[count].attrName = new String(tempIndexRec.attrName);
                      if (tempIndexRec.accessType == null || tempIndexRec.order == null) {
                          throw new IndexCatalogException(null, "Inconsistent data: Null accessType or order found.");
                      }
                      indexes[count].accessType = new IndexType(tempIndexRec.accessType.indexType);
                      indexes[count].order = new TupleOrder(tempIndexRec.order.tupleOrder);
                      indexes[count].clustered = tempIndexRec.clustered;
                      indexes[count].distinctKeys = tempIndexRec.distinctKeys;
                      indexes[count].indexPages = tempIndexRec.indexPages;
                      // *** Deep copy physicalFileName ***
                      indexes[count].physicalFileName = (tempIndexRec.physicalFileName != null) ? new String(tempIndexRec.physicalFileName) : null;
                      count++;
                  } else {
                      System.err.println("Warning: Found more indexes for " + relation + "." + attrName + " than expected (" + actualIndexCnt + ")");
                      break; // Avoid array out of bounds
                  }
                }
            } // End while

            if (count != actualIndexCnt) {
                // This might indicate an inconsistency between attrcat and indexcat
                System.err.println("Warning: Expected " + actualIndexCnt + " indexes for " + relation + "." + attrName + " based on attrcat, but found " + count + " in indexcat.");
                // Depending on requirements, you might throw Catalogindexnotfound or just return count
                // throw new Catalogindexnotfound(null, "Expected " + actualIndexCnt + " indexes for " + relation + "." + attrName + " based on attrcat, but found " + count + " in indexcat.");
            }

      } finally {
          if (pscan != null) {
              pscan.closescan();
          }
      }
      return count; // Return the actual number found and copied
    };
  
  // CREATES A FILE NAME FOR AN INDEX 
  public String buildIndexName(String relation, String attrName,
                   IndexType accessType)
    {
      String accessName = null;
      int sizeName;
      int sizeOfByte = 1;
      String indexName = null;

      // DETERMINE INDEX TYPE
      if(accessType.indexType == IndexType.B_Index)
        accessName = new String("B_Index");
      else if(accessType.indexType == IndexType.Hash)
        accessName = new String("Hash");
      // LSH index names are generated differently (in createIndex) and stored, not built here.

      // CHECK FOR LEGIT NAME SIZE (Only relevant for BTree/Hash now)
      if (accessName != null) {
          sizeName = relation.length() + accessName.length() +
            attrName.length() + (3 * sizeOfByte); // 3 hyphens

          // Check if combined length exceeds MAXNAME (or a more appropriate limit)
          // if(sizeName > MAXNAME) {
          //    System.err.println("Warning: Generated index name might exceed MAXNAME limit.");
          //    // Consider throwing an exception or truncating if necessary
          // }

          // CREATE NAME
          indexName = new String(relation);
          indexName = indexName.concat("-");
          indexName = indexName.concat(accessName);
          indexName = indexName.concat("-");
          indexName = indexName.concat(attrName);
      } else {
          // Handle cases where accessType is not BTree or Hash if needed.
          // For LSH, this method shouldn't ideally be called to get the *storage* name.
          // Returning null might be appropriate if called for LSH.
      }

      return indexName;
    };
  
  // ADD INDEX ENTRY TO CATALOG
  public void addInfo(IndexDesc record)
    throws IOException,
       IndexCatalogException
    {
      RID rid; // rid is not actually used here, but declared in original

      // Check if the member tuple is initialized
      if (this.tuple == null) {
          throw new IndexCatalogException(null, "Internal error: IndexCatalog member 'tuple' not initialized in addInfo.");
      }

      try {
        make_tuple(this.tuple, record); // Use the member tuple
      }
      catch (Exception e4) {
        throw new IndexCatalogException(e4, "make_tuple failed in addInfo");
      }

      try {
        insertRecord(this.tuple.getTupleByteArray()); // Use the member tuple
      }
      catch (Exception e) {
        throw new IndexCatalogException(e, "insertRecord() failed in addInfo");
      }
    };
  
  // REMOVE INDEX ENTRY FROM CATALOG
  public void removeInfo(String relation, String attrName,
             IndexType accessType)
    throws IOException, Catalogmissparam, Catalogindexnotfound, IndexCatalogException
    {
      if ((relation == null)||(attrName == null) || (accessType == null))
        throw new Catalogmissparam(null, "MISSING_PARAM relation, attrName, or accessType");

      RID rid = new RID();
      Scan pscan = null;
      IndexDesc record = new IndexDesc(); // Local record to read into
      boolean found = false;

      try {
          try {
            pscan = new Scan(this);
          } catch (Exception e) {
            throw new IndexCatalogException(e,"scan failed");
          }

          Tuple currentTuple = null; // Use a local Tuple for reading

          while (true)
            {
              try {
                currentTuple = pscan.getNext(rid);
                if (currentTuple == null) {
                  break; // End of scan
                }

                if (this.attrs == null || this.str_sizes == null) {
                    throw new IndexCatalogException(null, "Internal error: IndexCatalog schema not initialized.");
                }
                // *** Use NUM_FIELDS ***
                currentTuple.setHdr(NUM_FIELDS, this.attrs, this.str_sizes);

                // Initialize sub-objects if null before reading
                if (record.accessType == null) record.accessType = new IndexType(IndexType.None);
                if (record.order == null) record.order = new TupleOrder(TupleOrder.Ascending);
                read_tuple(currentTuple, record);
              }
              catch (Exception e4) {
                throw new IndexCatalogException(e4, "getNext, setHdr, or read_tuple failed");
              }

              // Check if this is the record to delete
              if(record.relName != null && record.attrName != null && record.accessType != null &&
                 record.relName.equalsIgnoreCase(relation)==true &&
                 record.attrName.equalsIgnoreCase(attrName)==true &&
                 (record.accessType.indexType == accessType.indexType))
                {
                  try {
                    deleteRecord(rid);  // FOUND - DELETE
                    found = true;
                  } catch (Exception e){
                    throw new IndexCatalogException(e, "deleteRecord() failed");
                  }
                  break; // Exit loop after deleting
                }
            } // End while

            if (!found) {
                throw new Catalogindexnotfound(null, "Catalog: Index not Found for " + relation + "." + attrName + " with type " + accessType.indexType + " for removal.");
            }

      } finally {
          if (pscan != null) {
              pscan.closescan();
          }
      }
    };



    public void addIndexCatalogEntry(IndexDesc record)
    throws IOException,
       IndexCatalogException,
           Catalogmissparam
    {
      // Check if the provided record is valid
      if (record == null || record.relName == null || record.attrName == null || record.accessType == null || record.order == null) {
          throw new Catalogmissparam(null, "Missing/invalid information in IndexDesc record for addIndexCatalogEntry");
      }
      // Also check physicalFileName if it's mandatory for certain index types
      if (record.accessType.indexType == IndexType.LSHFIndex && (record.physicalFileName == null || record.physicalFileName.isEmpty())) {
          // Allow adding LSH entry even if filename is temporarily missing, but log warning?
          // Or enforce it:
          // throw new Catalogmissparam(null, "Missing physicalFileName for LSHFIndex in addIndexCatalogEntry");
          System.err.println("Warning: Adding LSHFIndex entry without a physicalFileName in addIndexCatalogEntry for " + record.relName + "." + record.attrName);
      }


      // Create a LOCAL Tuple for this operation
      Tuple localTuple = new Tuple(Tuple.max_size); // Use max_size or calculate required size
      try {
          // Ensure the local tuple has the correct header
          if (this.attrs == null || this.str_sizes == null) {
             throw new IndexCatalogException(null, "Internal error: IndexCatalog schema (attrs/str_sizes) not initialized.");
          }
          // *** Use NUM_FIELDS ***
          localTuple.setHdr(NUM_FIELDS, this.attrs, this.str_sizes);
      } catch (Exception e_hdr) {
          throw new IndexCatalogException(e_hdr, "Failed to set header on local tuple in addIndexCatalogEntry");
      }

      try {
        // Convert the provided IndexDesc record into the LOCAL tuple format
        make_tuple(localTuple, record); // Use localTuple
      }
      catch (Exception e_make) {
        throw new IndexCatalogException(e_make, "make_tuple failed in addIndexCatalogEntry");
      }

      try {
        // Insert the LOCAL tuple into the IndexCatalog heap file
        insertRecord(localTuple.getTupleByteArray()); // Use localTuple
      }
      catch (Exception e_insert) {
        throw new IndexCatalogException(e_insert, "insertRecord() failed in addIndexCatalogEntry");
      }
    };







  
  // ADD INDEX TO A RELATION
//   public void addIndex(String relation, String attrName,
// 		       IndexType accessType, int buckets )
//     throws IOException,
// 	   Catalogioerror, 
// 	   Cataloghferror, 
// 	   Catalogmissparam,
// 	   Catalogattrnotfound, 
// 	   Catalogbadtype, 
// 	   Catalognomem,
// 	   Catalogindexnotfound, 
// 	   IndexCatalogException,
// 	   java.lang.Exception
//     {
//       RID    	rid = null;
//       IndexDesc indexRec = null;
//       AttrDesc  attrRec = null;
//       int   	intKey = 0;
//       float 	floatKey = (float) 0.0;
//       String	charKey = null;
//       int   	attrCnt = 0;
//       KeyClass key = null;      
//       int   	recSize = 0;
      
//       Heapfile datafile = null;
//       String	indexName = null;
//       Tuple 	tuple = null;
//       BTreeFile btree = null;
//       Scan 	pscan = null;
//       AttrType [] typeArray = null;
//       short 	[] sizeArray = null;
      
      
//       // CHECK PARM 
      
//       if ((relation == null)||(attrName == null))
// 	throw new Catalogmissparam(null, "MISSING_PARAM");
      
      
//       // CHECK FOR EXISTING INDEX
      
//       try {
// 	getInfo(relation, attrName, accessType, indexRec);
//       }
//       catch (Catalogioerror e) {
// 	System.err.println ("Catalog I/O Error!"+e);
// 	throw new Catalogioerror(null, "");
//       }
//       catch (Cataloghferror e1) {
// 	System.err.println ("Catalog Heapfile Error!"+e1);
// 	throw new Cataloghferror(null, "");
//       }
//       catch (Catalogmissparam e2) {
// 	System.err.println ("Catalog Missing Param Error!"+e2);
// 	throw new Catalogmissparam(null, "");
//       }
      
//       // GET ATTRIBUTE INFO 
      
//       try {
// 	ExtendedSystemDefs.MINIBASE_ATTRCAT.getInfo(relation, attrName, attrRec);
//       }
//       catch (Exception e2) {
// 	throw new IndexCatalogException(e2, "getInfo() failed");
//       }
      
//       // Can only index on int's and strings currently
//       if ((attrRec.attrType.attrType != AttrType.attrInteger) 
// 	  && (attrRec.attrType.attrType != AttrType.attrString)) 
// 	throw new Catalogbadtype(null, "Catalog: BAD TYPE!");
      
      
//       // UPDATE ATTRIBUTE INFO
      
//       attrRec.indexCnt++;
      
//       try {
// 	ExtendedSystemDefs.MINIBASE_ATTRCAT.removeInfo(relation, attrName);
// 	ExtendedSystemDefs.MINIBASE_ATTRCAT.addInfo(attrRec);
//       }
//       catch (Exception e) {
// 	throw new IndexCatalogException(e, "add/remove info failed");
//       }
      
      
//       // BUILD INDEX FILE NAME
      
//       indexName = buildIndexName(relation, attrName, accessType);
      
      
//       // ADDED BY BILL KIMMEL - DELETE LATER
//       System.out.println("Index name is " +indexName);
      
      
//       // IF BTREE
      
//       if (accessType.indexType == IndexType.B_Index)
// 	{
// 	  btree = new BTreeFile(indexName, attrRec.attrType.attrType, attrRec.attrLen, 0);
// 	} 
      
      
//       // ADD ENTRY IN INDEXCAT
      
      
//       indexRec.relName = new String(relation);
//       indexRec.attrName = new String(attrName);
//       indexRec.accessType = accessType;
      
//       if (accessType.indexType == IndexType.B_Index)
// 	indexRec.order = new TupleOrder(TupleOrder.Ascending);
//       else
// 	indexRec.order = new TupleOrder(TupleOrder.Random);
      
//       indexRec.distinctKeys = DISTINCTKEYS;
//       indexRec.clustered = 0;  // 0 means non-clustered!!!!
      
//       indexRec.indexPages  = INDEXPAGES;
      
//       try {
// 	addInfo(indexRec);
//       }
//       catch (Exception e) {
// 	throw new IndexCatalogException(e, "addInfo() failed");
//       }
      
      
//       // PREPARE TO SCAN DATA FILE
      
//       try {
// 	datafile = new Heapfile(relation);
// 	if (datafile == null)
// 	  throw new Catalognomem(null, "NO Enough Memory!");
//       }
//       catch (Exception e) {
// 	throw new IndexCatalogException(e, "create heapfile failed");
//       }
      
//       try {
// 	pscan = datafile.openScan();
//       }
//       catch (Exception e) {
// 	throw new IndexCatalogException(e,"openScan() failed");
//       }
      
      
//       // PREPARE TUPLE
      
//       try {
// 	ExtendedSystemDefs.MINIBASE_ATTRCAT.getTupleStructure(relation, attrCnt, typeArray, sizeArray);
//       }
//       catch (Exception e) {
// 	throw new IndexCatalogException(e,"getTupleStructure");
//       }
      
//       tuple = new Tuple(Tuple.max_size);
//       if (tuple == null)
// 	throw new Catalognomem(null, "Catalog, No Enough Memory!");
      
//       try {
// 	tuple.setHdr((short)attrCnt, typeArray, sizeArray);
//       }
//       catch (Exception e) {
// 	throw new IndexCatalogException(e, "setHdr() failed");
//       }

//       recSize = tuple.size();
      
      
//       // NOW PROCESS THE HEAPFILE AND INSERT KEY,RID INTO INDEX
      
//       while(true) {
// 	try {
// 	  tuple = pscan.getNext(rid);
// 	  if (tuple == null) 
// 	    return;
// 	}
// 	catch (Exception e) {
// 	  throw new IndexCatalogException(e, "getNext() failed");
// 	}
	
// 	// PULL OUT THE KEY VALUE FROM HEAPFILE RECORD
	
// 	if (attrRec.attrType.attrType == AttrType.attrInteger)
// 	  {
// 	    intKey = tuple.getIntFld(attrRec.attrPos);
// 	    key = new IntegerKey(intKey);
// 	  }
// 	else if (attrRec.attrType.attrType == AttrType.attrReal)
// 	  {
// 	    floatKey = tuple.getFloFld(attrRec.attrPos);
// 	    key = new IntegerKey((int)floatKey);
// 	  }
// 	else if (attrRec.attrType.attrType == AttrType.attrString)
// 	  {
// 	    charKey = new String(tuple.getStrFld(attrRec.attrPos));
// 	    key = new StringKey(charKey);
// 	  }
	
// 	// NOW INSERT RECORD INTO INDEX
	
// 	if (accessType.indexType == IndexType.B_Index) {
// 	  try {
// 	    btree.insert(key, rid); 
// 	  }
// 	  catch (Exception e) {
// 	    throw new IndexCatalogException(e, "insert failed");	
// 	  }
// 	}
//       }
      
//     };
  
  // DROP INDEX FROM A RELATION
  void dropIndex(String relation, String attrName,
		 IndexType accessType){};
  
  // DROP ALL INDEXES FOR A RELATION
  void dropRelation(String relation){};
  
  
  void make_tuple(Tuple tuple, IndexDesc record)
    throws IOException,
       IndexCatalogException
    {
      // Ensure tuple and record are not null
      if (tuple == null || record == null) {
          throw new IndexCatalogException(null, "Null parameter passed to make_tuple");
      }
      // Ensure sub-objects are initialized
      if (record.accessType == null || record.order == null) {
          throw new IndexCatalogException(null, "Null accessType or order in IndexDesc for make_tuple");
      }

      try {
        tuple.setStrFld(1, record.relName);
        tuple.setStrFld(2, record.attrName);

        // Handle accessType
        if (record.accessType.indexType == IndexType.None)
          tuple.setIntFld(3, 0);
        else if (record.accessType.indexType == IndexType.B_Index)
          tuple.setIntFld(3, 1);
        else if (record.accessType.indexType == IndexType.Hash)
          tuple.setIntFld(3, 2);
        else if (record.accessType.indexType == IndexType.LSHFIndex) // Use LSHFIndex
          tuple.setIntFld(3, 3); // Use code 3
        else
          throw new IndexCatalogException(null, "Invalid accessType in make_tuple: " + record.accessType.indexType);

        // Handle order
        if (record.order.tupleOrder == TupleOrder.Ascending)
          tuple.setIntFld(4, 0);
        else if (record.order.tupleOrder == TupleOrder.Descending)
          tuple.setIntFld(4, 1);
        else if (record.order.tupleOrder == TupleOrder.Random)
          tuple.setIntFld(4, 2);
        else
          throw new IndexCatalogException(null, "Invalid order in make_tuple: " + record.order.tupleOrder);

        tuple.setIntFld(5, record.clustered);
        tuple.setIntFld(6, record.distinctKeys);
        tuple.setIntFld(7, record.indexPages);

        // *** Write physicalFileName (handle null, ensure it fits MAXFILENAME) ***
        String filenameToWrite = (record.physicalFileName != null) ? record.physicalFileName : "";
        if (filenameToWrite.length() > MAXFILENAME) {
            System.err.println("Warning: physicalFileName '" + filenameToWrite + "' exceeds MAXFILENAME (" + MAXFILENAME + ") and will be truncated in IndexCatalog.make_tuple.");
            filenameToWrite = filenameToWrite.substring(0, MAXFILENAME);
        }
        tuple.setStrFld(8, filenameToWrite); // <-- ADDED

      }
      catch (Exception e) { // Catch specific like FieldNumberOutOfBoundException if possible
        throw new IndexCatalogException(e,"make_tuple failed setting field");
      }
    };
  
    void read_tuple(Tuple tuple, IndexDesc record)
    throws IOException,
       IndexCatalogException
    {
      // Ensure tuple and record are not null
      if (tuple == null || record == null) {
          throw new IndexCatalogException(null, "Null parameter passed to read_tuple");
      }
      // Ensure sub-objects are initialized if needed by getXXXFld
      if (record.accessType == null) record.accessType = new IndexType(IndexType.None);
      if (record.order == null) record.order = new TupleOrder(TupleOrder.Ascending);

      try {
        try { record.relName = tuple.getStrFld(1); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 1 (relName)"); }

        try { record.attrName = tuple.getStrFld(2); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 2 (attrName)"); }

        int tempAccessType;
        try { tempAccessType = tuple.getIntFld(3); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 3 (accessType code)"); }

        // Handle accessType code
        if (tempAccessType == 0)
          record.accessType.indexType = IndexType.None;
        else if (tempAccessType == 1)
          record.accessType.indexType = IndexType.B_Index;
        else if (tempAccessType == 2)
          record.accessType.indexType = IndexType.Hash;
        else if (tempAccessType == 3) // Check for code 3
          record.accessType.indexType = IndexType.LSHFIndex; // Use LSHFIndex
        else
          throw new IndexCatalogException(null, "Invalid accessType code (" + tempAccessType + ") found in field 3");


        int tempOrder;
        try { tempOrder = tuple.getIntFld(4); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 4 (order code)"); }

        // Handle order code
        if (tempOrder == 0)
          record.order.tupleOrder = TupleOrder.Ascending;
        else if (tempOrder == 1)
          record.order.tupleOrder = TupleOrder.Descending;
        else if (tempOrder == 2)
          record.order.tupleOrder = TupleOrder.Random;
        else
          throw new IndexCatalogException(null, "Invalid order code (" + tempOrder + ") found in field 4");


        try { record.clustered = tuple.getIntFld(5); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 5 (clustered)"); }

        try { record.distinctKeys = tuple.getIntFld(6); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 6 (distinctKeys)"); }

        try { record.indexPages = tuple.getIntFld(7); }
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 7 (indexPages)"); }

        // *** Read physicalFileName ***
        try { record.physicalFileName = tuple.getStrFld(8); } // <-- ADDED
        catch (Exception e) { throw new IndexCatalogException(e, "read_tuple failed on field 8 (physicalFileName)"); }


      }
      catch (IndexCatalogException e_read) { // Catch the specific exception thrown above
          System.err.println("--- Error reading index catalog tuple ---");
          try {
              byte[] data = tuple.getTupleByteArray();
              System.err.print("Tuple data (hex): ");
              for(int i=0; i<tuple.getLength(); i++) { System.err.printf("%02X ", data[i]); }
              System.err.println();
          } catch (Exception ignored) {}
          System.err.println("--------------------------------------");
          throw e_read; // Re-throw the detailed exception
      }
      catch (Exception e_other) { // Catch any other unexpected errors
        throw new IndexCatalogException(e_other, "Unexpected error in read_tuple");
      }
    };
  
  
  Tuple tuple;
  short [] str_sizes;
  AttrType [] attrs;
  int size;
};

