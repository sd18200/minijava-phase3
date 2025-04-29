//------------------------------------
// Catalog.java
//
// Ning Wang, April, 1998
//-------------------------------------

package catalog;

import java.io.*;

import global.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import catalog.attrData; // Add import for AttrData

public class Catalog
  implements GlobalConst, Catalogglobal
{

  // open relation catalog (invokes constructors for each)

  public Catalog()
    {

      System.out.println("DEBUG: Entering Catalog constructor...");
      boolean isNewDB = false; // Flag to check if we need to initialize

      try {
        // Check if the primary catalog file exists BEFORE trying to open/create it.
        // If get_file_entry returns null, the file doesn't exist in the DB directory.
        PageId relcatPageId = null;
        try {
             // Ensure JavabaseDB is initialized before calling this
             if (SystemDefs.JavabaseDB != null) {
                relcatPageId = SystemDefs.JavabaseDB.get_file_entry("relcatalog");
             } else {
                 // This case should ideally not happen if ExtendedSystemDefs calls super() first
                 System.err.println("WARNING: JavabaseDB is null during Catalog constructor check.");
             }
        } catch (Exception ignored) {
             // Ignore exceptions during this check; null means not found or error.
             System.err.println("DEBUG: Exception during get_file_entry check (likely okay if DB is new): " + ignored);
        }
        isNewDB = (relcatPageId == null);

        if (isNewDB) {
             System.out.println("DEBUG: Catalog constructor detected new database setup.");
        } else {
             System.out.println("DEBUG: Catalog constructor detected existing database setup.");
        }

        // RELCAT (Constructor will create the heap file if isNewDB is true)
        relCat = new RelCatalog("relcatalog");

        // ATTRCAT (Constructor will create the heap file if isNewDB is true)
        attrCat = new AttrCatalog("attrcatalog");

        // INDCAT (Constructor will create the heap file if isNewDB is true)
        indCat = new IndexCatalog("indexcatalog");

        if (indCat == null) {
          System.err.println("CRITICAL ERROR: Catalog constructor - indCat is NULL immediately after new IndexCatalog()!");
     } else if (indCat.tuple == null) { // Access tuple directly if possible, or add a getter
          System.err.println("CRITICAL ERROR: Catalog constructor - indCat.tuple is NULL immediately after new IndexCatalog()!");
     } else {
          System.out.println("DEBUG: Catalog constructor - indCat and indCat.tuple appear OK after construction.");
     }

        // *** If it's a new DB, populate the catalog schema ***
        if (isNewDB) {
            System.out.println("DEBUG: Calling Catalog.initialize() to bootstrap schema...");
            initialize(); // Call the method to populate catalog tables with their own schema
            System.out.println("DEBUG: Catalog.initialize() completed.");
            // Optional: Flush pages immediately to ensure catalog data is persistent
            try {
                if (SystemDefs.JavabaseBM != null) {
                    SystemDefs.JavabaseBM.flushAllPages();
                    System.out.println("DEBUG: Flushed buffer pool pages after catalog initialization.");
                }
            } catch (Exception flushEx) {
                 System.err.println("Error flushing pages after catalog init: " + flushEx);
                 flushEx.printStackTrace();
            }
        }

      }
      catch (Exception e) {
        System.err.println ("Fatal Error initializing Catalog: "+e);
        e.printStackTrace();
        // It's critical if catalog init fails, maybe rethrow a RuntimeException
         throw new RuntimeException("Catalog Initialization Failed", e);
      }
    }


  // get catalog entry for a relation
  void getRelationInfo(String relation, RelDesc record)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           RelCatalogException
    {
      relCat.getInfo(relation, record);
    };

  // create a new relation
  void createRel(String relation, int attrCnt, attrInfo [] attrList)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           RelCatalogException,
           AttrCatalogException
    {
      relCat.createRel( relation, attrCnt, attrList);
    };

  // destroy a relation
  void destroyRel(String relation)
    throws CatalogException // Added exception based on RelCatalog likely throwing it
    {
      // Need to handle potential exceptions from destroyRel
      try {
          relCat.destroyRel( relation);
      } catch (Exception e) {
          throw new CatalogException(e, "Error destroying relation in RelCatalog");
      }
    };

  // add a index to a relation
  // void addIndex(String relation, String attrname,
  //               IndexType accessType, int buckets) // Buckets might be relevant for Hash
  //   throws Catalogmissparam,
  //          Catalogrelexists,
  //          Catalogdupattrs,
  //          Catalognomem,
  //          IOException,
  //          CatalogException,
  //          Catalogioerror,
  //          Cataloghferror,
  //          Catalogrelnotfound,
  //          Catalogindexnotfound,
  //          Catalogattrnotfound,
  //          Catalogbadattrcount,
  //          Catalogattrexists,
  //          Catalogbadtype,
  //          java.lang.Exception // Keep generic Exception or make more specific
  //   {
  //     // The '0' passed here might be incorrect depending on what addIndex expects.
  //     // If buckets are needed for HashIndex, this needs adjustment.
  //     relCat.addIndex(relation, attrname, accessType, 0);
  //   };

  // drop an index from a relation
  void dropIndex(String relation, String attrname,
                 IndexType accessType)
    throws CatalogException // Added exception based on RelCatalog likely throwing it
    {
      // Need to handle potential exceptions from dropIndex
      try {
          relCat.dropIndex(relation, attrname, accessType);
      } catch (Exception e) {
          throw new CatalogException(e, "Error dropping index in RelCatalog");
      }
    };

  // get a catalog entry for an attribute
  void getAttributeInfo(String relation, String attrName,
                        AttrDesc record)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           AttrCatalogException
    {
      attrCat.getInfo(relation, attrName, record);
    };

  // get catalog entries for all attributes of a relation
  // return attrCnt.
  int getRelAttributes(String relation, int attrCnt,
                       AttrDesc [] attrs)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           AttrCatalogException
    {
      int count;
      count = attrCat.getRelInfo(relation, attrCnt, attrs);

      return count;
    };

  // get catalog entries for all indexes for a relation
  int getRelIndexes(String relation, int indexCnt,
                    IndexDesc [] indexes)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           IndexCatalogException,
           RelCatalogException
    {
      int count;
      count = indCat.getRelInfo(relation, indexCnt, indexes);

      return count;
    };

  // get catalog entries for all indexes for an attribute
  int getAttrIndexes(String relation, String attrName,
                     int indexCnt, IndexDesc [] indexes)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           IndexCatalogException
    {
      int count;
      count = indCat.getAttrIndexes(relation, attrName, indexCnt, indexes);

      return count;
    };

  // get catalog entry on an index
  void getIndexInfo(String relation, String attrName,
                    IndexType accessType, IndexDesc record)
    throws Catalogmissparam,
           Catalogrelexists,
           Catalogdupattrs,
           Catalognomem,
           IOException,
           CatalogException,
           Catalogioerror,
           Cataloghferror,
           Catalogrelnotfound,
           Catalogindexnotfound,
           Catalogattrnotfound,
           Catalogbadattrcount,
           Catalogattrexists,
           Catalogbadtype,
           Exception // Keep generic or make more specific (IndexCatalogException?)
    {
      indCat.getInfo(relation, attrName, accessType, record);
    };

  // dump catalogs to a disk file for optimizer
  void dumpCatalog(String filename)
    {
      // Consider adding exception handling/propagation
      relCat.dumpCatalog(filename);
    };

  // Runs stats on the database...
  void runStats(String filename)
    {
      // Consider adding exception handling/propagation
      relCat.runStats(filename);
    };

  // Lists relations - Implementation seems incomplete/incorrect
  void listRelations()
    throws CatalogException
    {
      System.out.println("\nListing Relations:");
      try {
        // This creates a Scan but doesn't use it to list anything.
        // Need to iterate through the scan and print relation names.
        Scan relscan = new Scan(relCat); // Assuming RelCatalog implements Scan interface or provides one
        RelDesc relRec = new RelDesc();
        RID rid = new RID();
        Tuple tuple;
        boolean done = false;
        int count = 0;

        while (!done) {
            tuple = relscan.getNext(rid);
            if (tuple == null) {
                done = true;
                break;
            }
            relRec.tupleToRelDesc(tuple); // Assuming RelDesc has this method
            System.out.println("  " + relRec.relName);
            count++;
        }
        relscan.closescan(); // Important to close the scan
        System.out.println("Total relations: " + count + "\n");

      }
      catch (Exception e1) {
        System.err.println("Error listing relations: " + e1);
        e1.printStackTrace();
        throw new CatalogException (e1, "scan failed during listRelations");
      }
    };

  // Initializes the catalog database (creates schema entries for catalog tables)
  void initialize()
  throws Catalogmissparam,
         // Catalogrelexists, // Not thrown if we use addInfo directly
         // Catalogdupattrs, // Should still check this manually if needed, but less likely for catalog tables
         Catalognomem,
         IOException,
         CatalogException,
         Catalogioerror,
         Cataloghferror,
         // Catalogrelnotfound, // Not thrown if we use addInfo directly
         // Catalogindexnotfound, // Not applicable here
         // Catalogattrnotfound, // Not thrown if we use addInfo directly
         Catalogbadattrcount,
         // Catalogattrexists, // Not thrown if we use addInfo directly
         Catalogbadtype,
         RelCatalogException,
         AttrCatalogException // Added AttrCatalogException
  {
    int max;
    int sizeOfInt = 4;
    int sizeOfFloat = 4; // Assuming float size is 4 bytes
    int currentOffset;

    // --- 1. Define Schema Structures (as before) ---
    // ... (schema definitions for relAttrs, attrAttrs, indAttrs remain the same) ...
    // Schema for relcatalog
    attrInfo [] relAttrs = new attrInfo[5];
    for(int i=0; i<relAttrs.length; i++) { relAttrs[i] = new attrInfo(); }
    relAttrs[0].attrType = new AttrType(AttrType.attrString); relAttrs[0].attrName = "relName"; relAttrs[0].attrLen = MAXNAME;
    relAttrs[1].attrType = new AttrType(AttrType.attrInteger); relAttrs[1].attrName = "attrCnt"; relAttrs[1].attrLen = sizeOfInt;
    relAttrs[2].attrType = new AttrType(AttrType.attrInteger); relAttrs[2].attrName = "indexCnt"; relAttrs[2].attrLen = sizeOfInt;
    relAttrs[3].attrType = new AttrType(AttrType.attrInteger); relAttrs[3].attrName = "numTuples"; relAttrs[3].attrLen = sizeOfInt;
    relAttrs[4].attrType = new AttrType(AttrType.attrInteger); relAttrs[4].attrName = "numPages"; relAttrs[4].attrLen = sizeOfInt;

    // Schema for attrcatalog
    attrInfo [] attrAttrs = new attrInfo[9];
    for(int i=0; i<attrAttrs.length; i++) { attrAttrs[i] = new attrInfo(); }
    attrAttrs[0].attrType = new AttrType(AttrType.attrString); attrAttrs[0].attrName = "relName"; attrAttrs[0].attrLen = MAXNAME;
    attrAttrs[1].attrType = new AttrType(AttrType.attrString); attrAttrs[1].attrName = "attrName"; attrAttrs[1].attrLen = MAXNAME;
    attrAttrs[2].attrType = new AttrType(AttrType.attrInteger); attrAttrs[2].attrName = "attrOffset"; attrAttrs[2].attrLen = sizeOfInt;
    attrAttrs[3].attrType = new AttrType(AttrType.attrInteger); attrAttrs[3].attrName = "attrPos"; attrAttrs[3].attrLen = sizeOfInt;
    attrAttrs[4].attrType = new AttrType(AttrType.attrInteger); attrAttrs[4].attrName = "attrType"; attrAttrs[4].attrLen = sizeOfInt;
    attrAttrs[5].attrType = new AttrType(AttrType.attrInteger); attrAttrs[5].attrName = "attrLen"; attrAttrs[5].attrLen = sizeOfInt;
    attrAttrs[6].attrType = new AttrType(AttrType.attrInteger); attrAttrs[6].attrName = "indexCnt"; attrAttrs[6].attrLen = sizeOfInt;
    max = 10; if (sizeOfInt > max) max = sizeOfInt; if (sizeOfFloat > max) max = sizeOfFloat;
    attrAttrs[7].attrType = new AttrType(AttrType.attrString); attrAttrs[7].attrName = "minVal"; attrAttrs[7].attrLen = max;
    attrAttrs[8].attrType = new AttrType(AttrType.attrString); attrAttrs[8].attrName = "maxVal"; attrAttrs[8].attrLen = max;

    // Schema for indexcatalog
    attrInfo [] indAttrs = new attrInfo[7];
    for(int i=0; i<indAttrs.length; i++) { indAttrs[i] = new attrInfo(); }
    indAttrs[0].attrType = new AttrType(AttrType.attrString); indAttrs[0].attrName = "relName"; indAttrs[0].attrLen = MAXNAME;
    indAttrs[1].attrType = new AttrType(AttrType.attrString); indAttrs[1].attrName = "attrName"; indAttrs[1].attrLen = MAXNAME;
    indAttrs[2].attrType = new AttrType(AttrType.attrInteger); indAttrs[2].attrName = "accessType"; indAttrs[2].attrLen = sizeOfInt;
    indAttrs[3].attrType = new AttrType(AttrType.attrInteger); indAttrs[3].attrName = "order"; indAttrs[3].attrLen = sizeOfInt;
    indAttrs[4].attrType = new AttrType(AttrType.attrInteger); indAttrs[4].attrName = "clustered"; indAttrs[4].attrLen = sizeOfInt;
    indAttrs[5].attrType = new AttrType(AttrType.attrInteger); indAttrs[5].attrName = "distinctKeys"; indAttrs[5].attrLen = sizeOfInt;
    indAttrs[6].attrType = new AttrType(AttrType.attrInteger); indAttrs[6].attrName = "indexPages"; indAttrs[6].attrLen = sizeOfInt;


    // --- 2. Insert Relation Definitions into relcatalog ---
    // ... (relCat.addInfo calls remain the same) ...
    System.out.println("DEBUG: Bootstrapping relcatalog entries using addInfo...");

    // Create RelDesc for relcatalog
    RelDesc relDescRel = new RelDesc();
    relDescRel.relName = RELCATNAME;
    relDescRel.attrCnt = 5;
    relDescRel.indexCnt = 0; // Initially no indexes on catalog tables
    relDescRel.numTuples = 0; // Start with 0, will be updated later
    relDescRel.numPages = 1; // Starts with at least one page (directory)
    relCat.addInfo(relDescRel); // Use addInfo directly
    System.out.println("DEBUG: Added relcatalog definition to relCat.");

    // Create RelDesc for attrcatalog
    RelDesc relDescAttr = new RelDesc();
    relDescAttr.relName = ATTRCATNAME;
    relDescAttr.attrCnt = 9;
    relDescAttr.indexCnt = 0;
    relDescAttr.numTuples = 0;
    relDescAttr.numPages = 1;
    relCat.addInfo(relDescAttr); // Use addInfo directly
    System.out.println("DEBUG: Added attrcatalog definition to relCat.");

    // Create RelDesc for indexcatalog
    RelDesc relDescIndex = new RelDesc();
    relDescIndex.relName = INDEXCATNAME;
    relDescIndex.attrCnt = 7;
    relDescIndex.indexCnt = 0;
    relDescIndex.numTuples = 0;
    relDescIndex.numPages = 1;
    relCat.addInfo(relDescIndex); // Use addInfo directly
    System.out.println("DEBUG: Added indexcatalog definition to relCat.");


    // --- 3. Insert Attribute Definitions into attrcatalog ---
    System.out.println("DEBUG: Bootstrapping attrcatalog entries using addInfo...");

    // Add attributes for relcatalog
    currentOffset = 0;
    for (int i = 0; i < relAttrs.length; i++) {
        AttrDesc attrRec = new AttrDesc();
        // *** FIX: Initialize minVal and maxVal ***
        attrRec.minVal = new attrData();
        attrRec.maxVal = new attrData();
        // *** END FIX ***
        attrRec.relName = RELCATNAME;
        attrRec.attrName = relAttrs[i].attrName;
        attrRec.attrOffset = currentOffset;
        attrRec.attrPos = i + 1;
        attrRec.attrType = relAttrs[i].attrType;
        attrRec.attrLen = relAttrs[i].attrLen;
        attrRec.indexCnt = 0;
        // Set min/max if necessary based on type (using defaults here)
        if (attrRec.attrType.attrType == AttrType.attrString) {
            attrRec.minVal.strVal = ""; // Or appropriate defaults
            attrRec.maxVal.strVal = "";
        } else if (attrRec.attrType.attrType == AttrType.attrInteger) {
            attrRec.minVal.intVal = Integer.MIN_VALUE;
            attrRec.maxVal.intVal = Integer.MAX_VALUE;
        } else if (attrRec.attrType.attrType == AttrType.attrReal) {
            attrRec.minVal.floatVal = Float.MIN_VALUE;
            attrRec.maxVal.floatVal = Float.MAX_VALUE;
        }
        attrCat.addInfo(attrRec); // Use addInfo directly
        currentOffset += relAttrs[i].attrLen;
    }
    System.out.println("DEBUG: Added attribute definitions for relcatalog to attrCat.");

    // Add attributes for attrcatalog
    currentOffset = 0;
    for (int i = 0; i < attrAttrs.length; i++) {
        AttrDesc attrRec = new AttrDesc();
        // *** FIX: Initialize minVal and maxVal ***
        attrRec.minVal = new attrData();
        attrRec.maxVal = new attrData();
        // *** END FIX ***
        attrRec.relName = ATTRCATNAME;
        attrRec.attrName = attrAttrs[i].attrName;
        attrRec.attrOffset = currentOffset;
        attrRec.attrPos = i + 1;
        attrRec.attrType = attrAttrs[i].attrType;
        attrRec.attrLen = attrAttrs[i].attrLen;
        attrRec.indexCnt = 0;
        // Set min/max if necessary
        if (attrRec.attrType.attrType == AttrType.attrString) {
            attrRec.minVal.strVal = "";
            attrRec.maxVal.strVal = "";
        } else if (attrRec.attrType.attrType == AttrType.attrInteger) {
            attrRec.minVal.intVal = Integer.MIN_VALUE;
            attrRec.maxVal.intVal = Integer.MAX_VALUE;
        } else if (attrRec.attrType.attrType == AttrType.attrReal) {
            attrRec.minVal.floatVal = Float.MIN_VALUE;
            attrRec.maxVal.floatVal = Float.MAX_VALUE;
        }
        attrCat.addInfo(attrRec); // Use addInfo directly
        currentOffset += attrAttrs[i].attrLen;
    }
    System.out.println("DEBUG: Added attribute definitions for attrcatalog to attrCat.");

    // Add attributes for indexcatalog
    currentOffset = 0;
    for (int i = 0; i < indAttrs.length; i++) {
        AttrDesc attrRec = new AttrDesc();
        // *** FIX: Initialize minVal and maxVal ***
        attrRec.minVal = new attrData();
        attrRec.maxVal = new attrData();
        // *** END FIX ***
        attrRec.relName = INDEXCATNAME;
        attrRec.attrName = indAttrs[i].attrName;
        attrRec.attrOffset = currentOffset;
        attrRec.attrPos = i + 1;
        attrRec.attrType = indAttrs[i].attrType;
        attrRec.attrLen = indAttrs[i].attrLen;
        attrRec.indexCnt = 0;
        // Set min/max if necessary
         if (attrRec.attrType.attrType == AttrType.attrString) {
            attrRec.minVal.strVal = "";
            attrRec.maxVal.strVal = "";
        } else if (attrRec.attrType.attrType == AttrType.attrInteger) {
            attrRec.minVal.intVal = Integer.MIN_VALUE;
            attrRec.maxVal.intVal = Integer.MAX_VALUE;
        } else if (attrRec.attrType.attrType == AttrType.attrReal) {
            attrRec.minVal.floatVal = Float.MIN_VALUE;
            attrRec.maxVal.floatVal = Float.MAX_VALUE;
        }
        attrCat.addInfo(attrRec); // Use addInfo directly
        currentOffset += indAttrs[i].attrLen;
    }
    System.out.println("DEBUG: Added attribute definitions for indexcatalog to attrCat.");

    System.out.println("DEBUG: Catalog bootstrapping complete using addInfo.");
  }// End of initialize()

  // --- Getters for sub-catalogs ---
  public IndexCatalog getIndCat()
    { return indCat; };
  public RelCatalog getRelCat()
    { return relCat; };
  public AttrCatalog getAttrCat()
    { return attrCat; };


  // --- Private fields ---
  private IndexCatalog  indCat;
  private RelCatalog    relCat;
  private AttrCatalog   attrCat;
}