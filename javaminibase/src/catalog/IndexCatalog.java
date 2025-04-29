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
  
  // OPEN INDEX CATALOG
  IndexCatalog(String filename)
    throws IOException, 
	   BufMgrException,
	   DiskMgrException,
	   Exception
         {
            // Call the Heapfile constructor
            super(filename); // This MUST be the first line
      
            // *** ADD DEBUG LOGGING ***
            System.out.println("DEBUG: IndexCatalog constructor - AFTER super(filename) call for: " + filename);
      
            try {
                // Initialize the tuple member variable
                tuple = new Tuple(Tuple.max_size);
                if (tuple == null) { // Check immediately after creation
                    System.err.println("CRITICAL ERROR: IndexCatalog constructor - tuple is NULL immediately after new Tuple()!");
                } else {
                    System.out.println("DEBUG: IndexCatalog constructor - AFTER tuple = new Tuple(). Tuple is NOT null.");
                }
      
                // Initialize schema arrays
                attrs = new AttrType[7];
                attrs[0] = new AttrType(AttrType.attrString); // relName
                attrs[1] = new AttrType(AttrType.attrString); // attrName
                attrs[2] = new AttrType(AttrType.attrInteger); // accessType
                attrs[3] = new AttrType(AttrType.attrInteger); // order
                attrs[4] = new AttrType(AttrType.attrInteger); // clustered
                attrs[5] = new AttrType(AttrType.attrInteger); // distinctKeys
                attrs[6] = new AttrType(AttrType.attrInteger); // indexPages
      
                str_sizes = new short[2];
                str_sizes[0] = (short)MAXNAME; // relName
                str_sizes[1] = (short)MAXNAME; // attrName
      
                // Set the header for the tuple
                tuple.setHdr((short) 7, attrs, str_sizes);
                System.out.println("DEBUG: IndexCatalog constructor - AFTER tuple.setHdr().");
      
                size = tuple.size(); // Set the size based on the header
      
            } catch (Exception e) {
                System.err.println("CRITICAL ERROR: Exception during IndexCatalog tuple/header initialization!");
                e.printStackTrace();
                // Rethrow or handle appropriately - this object might be unusable
                throw new RuntimeException("Failed to initialize IndexCatalog tuple/header", e);
            }
      
            System.out.println("DEBUG: IndexCatalog constructor - COMPLETED for: " + filename);
            // *** END DEBUG LOGGING ***
          }
  
  // GET ALL INDEXES FOR A RELATION
  // Return indexCnt.
  public int getRelInfo(String relation, int indexCnt, IndexDesc [] indexes)
    throws Catalogmissparam, 
	   Catalogioerror, 
	   Cataloghferror, 
	   Catalogindexnotfound,
	   IOException, 
	   Catalognomem, 
	   Catalogattrnotfound,
	   IndexCatalogException,
	   RelCatalogException,
	   Catalogrelnotfound
    {
      RelDesc record = null;
      int status;
      int recSize;
      RID rid = null;
      Scan pscan = null;
      int count = 0;
      
      if (relation == null)
	throw new Catalogmissparam(null, "MISSING_PARAM");
      
      try {
	ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relation, record);
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
      catch (Catalogrelnotfound e3) {
	System.err.println ("Catalog: Relation not Found!"+e3);
	throw new Catalogrelnotfound(null, "");
      }
      
      // SET INDEX COUNT BY REFERENCE 
      
      indexCnt = record.indexCnt;
      
      if (indexCnt == 0)
	return indexCnt;
      
      
      // OPEN SCAN
      
      try {
	pscan = new Scan(this);
      }
      catch (Exception e) {
	throw new IndexCatalogException(e,"scan() failed");
      }
      
      // ALLOCATE INDEX ARRAY
      
      indexes = new IndexDesc[indexCnt];
      if (indexes == null)
	throw new Catalognomem(null, "Catalog: No Enough Memory!");
      
      // SCAN THE FILE
      
      while(true) 
	{
	  try {
	    tuple = pscan.getNext(rid);
	    if (tuple == null) 
	      throw new Catalogindexnotfound(null,
					     "Catalog: Index not Found!");
	    read_tuple(tuple, indexes[count]);
	  }
	  catch (Exception e4) {
	    throw new IndexCatalogException(e4," read_tuple() failed");
	  }
	  
	  if(indexes[count].relName.equalsIgnoreCase(relation)==true)
	    count++;
	  
	  if(count == indexCnt)  // IF ALL INDEXES FOUND
	    break;
	}
      
      return indexCnt;
      
    };
  
  // RETURN INFO ON AN INDEX
  public void getInfo(String relation, String attrName,
              IndexType accessType, IndexDesc record)
    throws Catalogmissparam,
       Catalogioerror, // Keep if Scan throws it
       Cataloghferror, // Keep if Scan throws it
       IOException,
       Catalogindexnotfound, // *** FIX: Throw this on not found ***
       IndexCatalogException // Use this for internal errors
    {
      // int recSize; // Unused
      RID rid = new RID(); // *** FIX: Initialize RID ***
      Scan pscan = null;
      boolean indexFound = false; // Flag to track if index is found

      // Check parameters
      if ((relation == null)||(attrName == null)||(record == null)) // Added record null check
    throw new Catalogmissparam(null, "MISSING_PARAM in IndexCatalog.getInfo");

      // Ensure record's internal types are initialized if needed by read_tuple
      // (Assuming IndexDesc constructor or caller handles this)
      // record.accessType = new IndexType(IndexType.None); // Example if needed
      // record.order = new TupleOrder(TupleOrder.Ascending); // Example if needed

      try { // *** FIX: Use try-finally for scan ***
          // OPEN SCAN
          try {
            pscan = new Scan(this); // 'this' is the IndexCatalog heapfile
          }
          catch (Exception e1) { // Catch specific exceptions like IOException, HFException etc. if possible
            // e1.printStackTrace(); // Optional logging
            throw new IndexCatalogException(e1, "Scan failed on indexcat");
          }

          // SCAN FILE
          while (true)
            {
              try {
                // Use the member variable 'tuple' declared in IndexCatalog class
                tuple = pscan.getNext(rid);
                if (tuple == null) {
                  // *** FIX: End of scan, break loop ***
                  break;
                }

                // *** FIX: Set header before reading (using schema from constructor) ***
                tuple.setHdr((short)7, attrs, str_sizes);

                // *** FIX: Read into the provided record object ***
                read_tuple(tuple, record); // Assumes read_tuple handles potential nulls in record's members
              }
              catch (Exception e4) { // Catch specific exceptions if possible
                // e4.printStackTrace(); // Optional logging
                throw new IndexCatalogException(e4, "read_tuple or getNext failed during indexcat scan");
              }

              // Check if this record matches the requested index
              // *** FIX: Compare types correctly and add null checks ***
              if(record.relName != null && record.attrName != null && record.accessType != null && // Null checks
                 record.relName.equalsIgnoreCase(relation)==true
                 && record.attrName.equalsIgnoreCase(attrName)==true
                 && (record.accessType.indexType == accessType.indexType)) // Compare type values
                {
                  indexFound = true; // FOUND
                  break; // Exit the loop
                }
            } // End While

            // *** FIX: Check flag after loop ***
            if (!indexFound) {
                // If loop finished and index was not found, throw Catalogindexnotfound
                throw new Catalogindexnotfound(null,"Catalog: Index not Found for " + relation + "." + attrName);
            }
            // If indexFound is true, the 'record' object is already populated by read_tuple

      } finally {
          // *** FIX: Ensure scan is closed ***
          if (pscan != null) {
              pscan.closescan();
          }
      }
      // No explicit return needed for void method
    };
  
  // GET ALL INDEXES INLUDING A SPECIFIED ATTRIBUTE
  public int getAttrIndexes(String relation,
			    String attrName, int indexCnt, IndexDesc [] indexes)
    throws Catalogmissparam, 
	   Catalogioerror, 
	   Cataloghferror,
	   IOException, 
	   Catalognomem,
	   Catalogindexnotfound,
	   Catalogattrnotfound,
	   IndexCatalogException
    {
      AttrDesc record = null;
      int status;
      int recSize;
      RID rid = null;
      Scan pscan = null;
      int count = 0;
      
      if (relation == null)
	throw new Catalogmissparam(null, "MISSING_PARAM");
      
      try {
	ExtendedSystemDefs.MINIBASE_ATTRCAT.getInfo(relation, attrName, record);
      }
      catch (Catalogioerror e) {
	throw e;
      }
      catch (Cataloghferror e1) {
	throw e1;
      }
      catch (Catalogmissparam e2) {
	throw e2;
      }
      catch (Catalogattrnotfound e3) {
	throw e3;
      }
      catch (Exception e4) {
	throw new IndexCatalogException (e4,"getInfo() failed");
      }
      
      // ASSIGN INDEX COUNT
      
      indexCnt = record.indexCnt;
      if(indexCnt == 0)
	return 0;
      
      // OPEN SCAN
      
      try {
	pscan = new Scan(this);
      }
      catch (Exception e) {
	throw new IndexCatalogException(e,"scan failed");
      }
      
      // ALLOCATE INDEX ARRAY
      
      indexes = new IndexDesc[indexCnt];
      if (indexes == null)
	throw new Catalognomem(null, "Catalog: No Enough Memory!");
      
      // SCAN FILE
      
      while(true) 
	{
	  try {
	    tuple = pscan.getNext(rid);
	    if (tuple == null) 
	      throw new Catalogindexnotfound(null,
					     "Catalog: Index not Found!");
	    read_tuple(tuple, indexes[count]);
	  }
	  catch (Exception e4) {
	    throw new IndexCatalogException(e4, "pascan.getNext() failed");
	  }
	  
	  if(indexes[count].relName.equalsIgnoreCase(relation)==true 
	     && indexes[count].attrName.equalsIgnoreCase(attrName)==true)
	    count++;
	  
	  if(count == indexCnt)  // if all indexes found
	    break; 
	}
      
      return indexCnt;    
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
      
      // CHECK FOR LEGIT NAME SIZE
      
      sizeName = relation.length() + accessName.length() +
	attrName.length() + (3 * sizeOfByte);
      
      //if(sizeName > MAXNAME)
      //    return MINIBASE_FIRST_ERROR( CATALOG, Catalog::INDEX_NAME_TOO_LONG );
      
      // CREATE NAME
      
      indexName = new String(relation);
      indexName = indexName.concat("-");
      indexName = indexName.concat(accessName);
      indexName = indexName.concat("-");
      indexName = indexName.concat(attrName);
      
      return indexName;    
    };
  
  // ADD INDEX ENTRY TO CATALOG
  public void addInfo(IndexDesc record)
    throws IOException,
	   IndexCatalogException
    {
      RID rid;
      
      try {
	make_tuple(tuple, record);
      }
      catch (Exception e4) {
	throw new IndexCatalogException(e4, "make_tuple failed");
      }
      
      try {
	insertRecord(tuple.getTupleByteArray());
      }
      catch (Exception e) {
	throw new IndexCatalogException(e, "insertRecord() failed");
      }
    };
  
  // REMOVE INDEX ENTRY FROM CATALOG
  public void removeInfo(String relation, String attrName,
			 IndexType accessType)
    throws IOException, 
	   Catalogmissparam, 
	   Catalogattrnotfound,
	   IndexCatalogException
    {
      int recSize;
      RID rid = null;
      Scan pscan = null;
      IndexDesc record = null;
      
      if ((relation == null)||(attrName == null))
	throw new Catalogmissparam(null, "MISSING_PARAM");
      
      // OPEN SCAN
      try {
	pscan = new Scan(this);
      }
      catch (Exception e) {
	throw new IndexCatalogException(e,"scan failed");
      }
      
      // SCAN FILE
      
      while (true)
	{
	  try {
	    tuple = pscan.getNext(rid);
	    if (tuple == null) 
	      throw new Catalogattrnotfound(null,
					    "Catalog: Attribute not Found!");
	    read_tuple(tuple, record);
	  }
	  catch (Exception e4) {
	    throw new IndexCatalogException(e4, "read_tuple failed");
	  }
	  
	  if(record.relName.equalsIgnoreCase(relation)==true 
	     && record.attrName.equalsIgnoreCase(attrName)==true
	     && (record.accessType == accessType))
	    {
	      try {
		deleteRecord(rid);  //  FOUND -  DELETE        
	      }
	      catch (Exception e){
		throw new IndexCatalogException(e, "deleteRecord() failed");
	      }
	      break; 
	    }
	}
      
      return;    
    };



    public void addIndexCatalogEntry(IndexDesc record)
    throws IOException,
       IndexCatalogException,
           Catalogmissparam // Add other specific exceptions make_tuple/insertRecord might throw
    {
      // Check if the provided record is valid
      if (record == null || record.relName == null || record.attrName == null || record.accessType == null || record.order == null) {
          throw new Catalogmissparam(null, "Missing/invalid information in IndexDesc record for addIndexCatalogEntry");
      }

      // *** Create a LOCAL Tuple for this operation ***
      Tuple localTuple = new Tuple(Tuple.max_size);
      try {
          // Ensure the local tuple has the correct header
          // (Assuming attrs and str_sizes are member variables initialized in constructor)
          if (this.attrs == null || this.str_sizes == null) {
             throw new IndexCatalogException(null, "Internal error: IndexCatalog schema (attrs/str_sizes) not initialized.");
          }
          localTuple.setHdr((short) 7, this.attrs, this.str_sizes);
      } catch (Exception e_hdr) {
          throw new IndexCatalogException(e_hdr, "Failed to set header on local tuple in addIndexCatalogEntry");
      }
      // *** END LOCAL Tuple Creation ***


      // *** DEBUG Check (can be removed later) ***
      // if (this.tuple == null) {
      //     System.err.println("DEBUG: addIndexCatalogEntry - Member 'tuple' is null before calling make_tuple (as expected now).");
      // }
      // *** END DEBUG ***

      try {
        // Convert the provided IndexDesc record into the LOCAL tuple format
        make_tuple(localTuple, record); // Use localTuple
      }
      catch (Exception e_make) { // Catch specific exceptions if possible
        // e_make.printStackTrace(); // Optional logging
        throw new IndexCatalogException(e_make, "make_tuple failed in addIndexCatalogEntry");
      }

      try {
        // Insert the LOCAL tuple into the IndexCatalog heap file
        insertRecord(localTuple.getTupleByteArray()); // Use localTuple
      }
      catch (Exception e_insert) { // Catch specific exceptions like HFException, etc.
        // e_insert.printStackTrace(); // Optional logging
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
       IndexCatalogException // Use specific exception
    {
      try {
    tuple.setStrFld(1, record.relName);
    tuple.setStrFld(2, record.attrName);

    // *** FIX: Handle LSHFIndex and throw exception for invalid type ***
    if (record.accessType.indexType == IndexType.None)
      tuple.setIntFld(3, 0);
    else if (record.accessType.indexType == IndexType.B_Index)
      tuple.setIntFld(3, 1);
    else if (record.accessType.indexType == IndexType.Hash)
      tuple.setIntFld(3, 2);
    else if (record.accessType.indexType == IndexType.LSHFIndex) // *** USE LSHFIndex ***
      tuple.setIntFld(3, 3); // *** Use code 3 ***
    else
      // System.out.println("Invalid accessType in IndexCatalog::make_tupl"); // Old way
      throw new IndexCatalogException(null, "Invalid accessType in make_tuple: " + record.accessType.indexType); // *** Throw exception ***

    // *** FIX: Throw exception for invalid order ***
    if (record.order.tupleOrder == TupleOrder.Ascending)
      tuple.setIntFld(4, 0);
    else if (record.order.tupleOrder == TupleOrder.Descending)
      tuple.setIntFld(4, 1);
    else if (record.order.tupleOrder == TupleOrder.Random)
      tuple.setIntFld(4, 2);
    else
      // System.out.println("Invalid order in IndexCatalog::make_tuple"); // Old way
      throw new IndexCatalogException(null, "Invalid order in make_tuple: " + record.order.tupleOrder); // *** Throw exception ***

    tuple.setIntFld(5, record.clustered);
    tuple.setIntFld(6, record.distinctKeys);
    tuple.setIntFld(7, record.indexPages);
      }
      catch (Exception e) { // Catch specific like FieldNumberOutOfBoundException if possible
        // e.printStackTrace(); // Optional logging
    throw new IndexCatalogException(e,"make_tuple failed");
      }
      // return; // Not needed for void
    };
  
    void read_tuple(Tuple tuple, IndexDesc record)
    throws IOException,
       IndexCatalogException // Use specific exception
    {
      try {
    record.relName = tuple.getStrFld(1);
    record.attrName = tuple.getStrFld(2);

    int temp;
    temp = tuple.getIntFld(3);
    // *** FIX: Handle LSHFIndex and throw exception for invalid type code ***
    if (temp == 0)
      record.accessType = new IndexType(IndexType.None);
    else if (temp == 1)
      record.accessType = new IndexType(IndexType.B_Index);
    else if (temp == 2)
      record.accessType = new IndexType(IndexType.Hash);
    else if (temp == 3) // *** Check for code 3 ***
      record.accessType = new IndexType(IndexType.LSHFIndex); // *** USE LSHFIndex ***
    else
      // System.out.println("111Error in IndexCatalog::read_tuple"); // Old way
      throw new IndexCatalogException(null, "Invalid accessType code in read_tuple: " + temp); // *** Throw exception ***

    temp = tuple.getIntFld(4);
    // *** FIX: Throw exception for invalid order code ***
    if (temp == 0)
      record.order = new TupleOrder(TupleOrder.Ascending);
    else if (temp == 1)
      record.order = new TupleOrder(TupleOrder.Descending);
    else if (temp == 2)
      record.order = new TupleOrder(TupleOrder.Random);
    else
      // System.out.println("222Error in IndexCatalog::read_tuple"); // Old way
      throw new IndexCatalogException(null, "Invalid order code in read_tuple: " + temp); // *** Throw exception ***

    record.clustered = tuple.getIntFld(5);
    record.distinctKeys = tuple.getIntFld(6);
    record.indexPages = tuple.getIntFld(7);
      }
      catch (Exception e) { // Catch specific like FieldNumberOutOfBoundException if possible
        // e.printStackTrace(); // Optional logging
    throw new IndexCatalogException(e,"read_tuple failed");
      }
      // return ; // Not needed for void
    };
  
  
  Tuple tuple;
  short [] str_sizes;
  AttrType [] attrs;
  int size;
};

