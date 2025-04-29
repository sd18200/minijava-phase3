package LSHFIndex;

import java.io.*;
import java.util.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import catalog.*;
import index.*;
import heap.*;
import btree.*;
import LSHFIndex.*;
import iterator.*;

/**
 * DBInterface - A command-line interface to the MiniBase database.
 * This class handles database creation, table management, and query execution
 * as specified in Phase 3, Task 2 of the project.
 */
public class DBInterface {


    private static class OuterQueryResult {
        iterator.Iterator iterator;
        AttrType[] attrTypes;
        short[] strSizes;
        int numAttrs;

        OuterQueryResult(iterator.Iterator iter, AttrType[] types, short[] sizes, int count) {
            this.iterator = iter;
            this.attrTypes = types;
            this.strSizes = sizes;
            this.numAttrs = count;
        }
    }

    
    // Current database state
    private static String currentDBName = null;
    private static boolean dbOpen = false;
    
    // Constants for buffer management
    private static final int DEFAULT_DB_PAGES = GlobalConst.MINIBASE_DB_SIZE;
    private static final int DEFAULT_BUFFER_PAGES = GlobalConst.NUMBUF;
    /**
     * Main method - entry point for the DB interface.
     * Parses commands and dispatches to appropriate handlers.
     */
    public static void main(String[] args) {
        System.out.println("MiniBase Database Interface");
        System.out.println("Type 'help' for command list");
        
        // Create a scanner for user input
        Scanner scanner = new Scanner(System.in);
        
        boolean exit = false;
        while (!exit) {
            System.out.print("DB> ");
            String line = scanner.nextLine().trim();
            
            // Skip empty lines
            if (line.isEmpty()) {
                continue;
            }
            
            // Parse the command
            String[] tokens = line.split("\\s+");
            String command = tokens[0].toLowerCase();
            
            try {
                switch (command) {
                    case "open":
                        if (tokens.length < 3 || !tokens[1].equalsIgnoreCase("database")) {
                            System.out.println("Usage: open database DBNAME");
                        } else {
                            openDatabase(tokens[2]);
                        }
                        break;
                        
                    case "close":
                        if (tokens.length < 2 || !tokens[1].equalsIgnoreCase("database")) {
                            System.out.println("Usage: close database");
                        } else {
                            closeDatabase();
                        }
                        break;
                        
                    case "batchcreate":
                        if (tokens.length < 3) {
                            System.out.println("Usage: batchcreate DATAFILENAME RELNAME");
                        } else {
                            batchCreate(tokens[1], tokens[2]);
                        }
                        break;
                        case "createindex":
                        if (tokens.length != 5) {
                            System.out.println("Usage: createindex RELNAME COLUMNID L h");
                        } else {
                            try {
                                String relationName = tokens[1];
                                int columnId = Integer.parseInt(tokens[2]);
                                int lValue = Integer.parseInt(tokens[3]);
                                int hValue = Integer.parseInt(tokens[4]);
                                createIndex(relationName, columnId, lValue, hValue);
                            } catch (NumberFormatException e) {
                                System.err.println("Error: Column ID, L, and h must be integers.");
                            } catch (Exception e) {
                                System.err.println("Error creating index: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }
                        break;
                    case "help":
                        printHelp();
                        break;
                        
                    case "exit":
                        exit = true;
                        // Close database if open
                        if (dbOpen) {
                            closeDatabase();
                        }
                        break;
                    case "batchinsert":
                        if (tokens.length < 3) {
                            System.out.println("Usage: batchinsert UPDATEFILENAME RELNAME");
                        } else {
                            batchInsert(tokens[1], tokens[2]);
                        }
                        break;
                    case "batchdelete":
                        if (tokens.length < 3) {
                            System.out.println("Usage: batchdelete UPDATEFILENAME RELNAME");
                        } else {
                            batchDelete(tokens[1], tokens[2]);
                        }
                        break;
                    case "query":
                        if (tokens.length != 5) {
                            System.out.println("Usage: query RELNAME1 RELNAME2 QSNAME NUMBUF");
                        } else {
                            try {
                                String relName1 = tokens[1];
                                String relName2 = tokens[2];
                                String queryFileName = tokens[3];
                                int bufferPages = Integer.parseInt(tokens[4]);
                                executeQuery(relName1, relName2, queryFileName, bufferPages);
                            } catch (NumberFormatException e) {
                                System.err.println("Error: NUMBUF must be an integer.");
                            } catch (Exception e) {
                                System.err.println("Error executing query: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }
                        break;


                        
                    default:
                        System.out.println("Unknown command: " + command);
                        System.out.println("Type 'help' for command list");
                }
            } catch (Exception e) {
                System.err.println("Error executing command: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        scanner.close();
        System.out.println("Database interface terminated.");
    }

    /**
     * Open a database with the given name.
     * If the database doesn't exist, create it.
     * 
     * @param dbname Name of the database to open
     * @throws Exception if there's an error opening/creating the database
     */
    private static void openDatabase(String dbname) throws Exception {
        if (dbOpen) {     
        System.out.println("Error: Another database is already open. Close it first.");
         return; }
        PCounter.initialize();
        try {
            System.out.println("DEBUG: Attempting to open/create database: " + dbname);
            // Try to open existing first
            boolean dbExists = true;
            try {
                 System.out.println("DEBUG: Trying to instantiate ExtendedSystemDefs for existing DB: " + dbname);
                 // Use 0 pages to check existence without creating if it fails immediately
                 new ExtendedSystemDefs(dbname, 0, DEFAULT_BUFFER_PAGES, "Clock");
                 System.out.println("DEBUG: Successfully instantiated ExtendedSystemDefs for existing DB.");
                 System.out.println("Opening existing database: " + dbname);
            } catch (Exception e_open) {
                 // Analyze the exception to see if it's *really* just "not found"
                 System.err.println("DEBUG: Exception caught while trying to open existing DB: " + e_open.getClass().getName() + " - " + e_open.getMessage());
                 // e_open.printStackTrace(); // Uncomment for full stack trace if needed
    
                 // Assume DB doesn't exist ONLY if it's a specific, expected exception
                 // (FileNotFoundException might be too specific, maybe check for HFDiskMgrException related to file not found?)
                 // For now, let's assume any exception here means we should try creating.
                 dbExists = false;
            }
    
            if (!dbExists) {
                System.out.println("Database may not exist or failed to open. Attempting to create new database: " + dbname);
                try {
                    System.out.println("DEBUG: Trying to instantiate ExtendedSystemDefs for NEW DB: " + dbname);
                    new ExtendedSystemDefs(dbname, DEFAULT_DB_PAGES, DEFAULT_BUFFER_PAGES, "Clock");
                    System.out.println("DEBUG: Successfully instantiated ExtendedSystemDefs for NEW DB.");
                    System.out.println("Created new database: " + dbname);
                } catch (Exception e_create) {
                    System.err.println("CRITICAL ERROR: Failed to create new database after open attempt failed!");
                    e_create.printStackTrace();
                    throw new Exception("Failed to create new database: " + dbname, e_create);
                }
            }
    
            currentDBName = dbname;
            dbOpen = true; // Set only after successful instantiation
    
            System.out.println("Database opened successfully.");
            System.out.println("Page reads: " + PCounter.getRCount());
            System.out.println("Page writes: " + PCounter.getWCount());
    
        } catch (Exception e) { // Catch any other unexpected errors during the process
            System.err.println("Error in openDatabase overall logic: " + e.getMessage());
            dbOpen = false; // Ensure DB is marked as not open on failure
            currentDBName = null;
            // Clean up SystemDefs? Might be risky if partially initialized.
            throw e; // Re-throw
        }
    }
    
    /**
     * Close the currently open database.
     * 
     * @throws Exception if there's an error closing the database
     */
    private static void closeDatabase() throws Exception {
        if (!dbOpen) {
            System.out.println("No database is currently open.");
            return;
        }
        
        // Reset page counter to track I/O operations
        PCounter.initialize();
        
        try {
            // Close the database and make it persistent
            if (SystemDefs.JavabaseBM != null) {
                SystemDefs.JavabaseBM.flushAllPages();
            }
            if (SystemDefs.JavabaseDB != null) {
                SystemDefs.JavabaseDB.closeDB();
            }
            
            // Update state
            currentDBName = null;
            dbOpen = false;
            
            // Print I/O statistics
            System.out.println("Database closed successfully.");
            System.out.println("Page reads: " + PCounter.getRCount());
            System.out.println("Page writes: " + PCounter.getWCount());
            
        } catch (Exception e) {
            System.err.println("Error closing database: " + e.getMessage());
            throw e;
        }
    }
    
    /**
     * Create a new table from a data file.
     * 
     * @param dataFileName Name of the data file
     * @param relationName Name of the relation to create
     * @throws Exception if there's an error creating the table
     */
    private static void batchCreate(String dataFileName, String relationName) throws Exception {
        System.out.println("DEBUG: Starting batchCreate for " + dataFileName + " to relation " + relationName);
        
        if (!dbOpen) {
            System.out.println("Error: No database is open. Please open a database first.");
            return;
        }
        
        // Reset page counter to track I/O operations
        PCounter.initialize();
        System.out.println("DEBUG: PCounter initialized");
        
        BufferedReader reader = null;
        Heapfile heapFile = null;
        
        try {
            // Open the data file
            System.out.println("DEBUG: Attempting to open data file: " + dataFileName);
            try {
                reader = new BufferedReader(new FileReader(dataFileName));
                System.out.println("DEBUG: Data file opened successfully");
            } catch (FileNotFoundException e) {
                System.err.println("ERROR: Data file not found: " + dataFileName);
                throw e;
            }
            
            // Read the number of attributes
            System.out.println("DEBUG: Reading attribute count...");
            String attrCountLine = reader.readLine();
            System.out.println("DEBUG: Read attribute count line: '" + attrCountLine + "'");
            
            int attrCount = Integer.parseInt(attrCountLine.trim());
            System.out.println("DEBUG: Attribute count parsed as: " + attrCount);
            
            if (attrCount <= 0) {
                throw new Exception("Invalid attribute count in data file: " + attrCount);
            }
            
            // Read the attribute types
            System.out.println("DEBUG: Reading attribute types...");
            String typesLine = reader.readLine();
            System.out.println("DEBUG: Read types line: '" + typesLine + "'");
            
            String[] typeStrs = typesLine.trim().split("\\s+");
            System.out.println("DEBUG: Found " + typeStrs.length + " type values");
            
            if (typeStrs.length != attrCount) {
                throw new Exception("Mismatch between attribute count (" + attrCount + 
                                  ") and type count (" + typeStrs.length + ")");
            }
            
            // Convert type strings to AttrType array
            System.out.println("DEBUG: Converting attribute types...");
            AttrType[] attrTypes = new AttrType[attrCount];
            System.out.println("DEBUG: Attribute types array created with size " + attrCount);
            
            for (int i = 0; i < attrCount; i++) {
                int typeInt = Integer.parseInt(typeStrs[i]);
                System.out.println("DEBUG: Attribute " + (i+1) + " has type code " + typeInt);
                
                switch (typeInt) {
                    case 1: // Integer
                        attrTypes[i] = new AttrType(AttrType.attrInteger);
                        System.out.println("DEBUG: Attribute " + (i+1) + " set as INTEGER");
                        break;
                    case 2: // Float/Real
                        attrTypes[i] = new AttrType(AttrType.attrReal);
                        System.out.println("DEBUG: Attribute " + (i+1) + " set as REAL");
                        break;
                    case 3: // String
                        attrTypes[i] = new AttrType(AttrType.attrString);
                        System.out.println("DEBUG: Attribute " + (i+1) + " set as STRING");
                        break;
                    case 4: // Vector
                        attrTypes[i] = new AttrType(AttrType.attrVector100D);
                        System.out.println("DEBUG: Attribute " + (i+1) + " set as VECTOR100D");
                        break;
                    default:
                        System.out.println("ERROR: Invalid attribute type: " + typeInt);
                        throw new Exception("Invalid attribute type: " + typeInt);
                }
            }
            
            // Count string attributes for size array
            System.out.println("DEBUG: Counting string attributes...");
            int strCount = 0;
            for (AttrType type : attrTypes) {
                if (type.attrType == AttrType.attrString) {
                    strCount++;
                }
            }
            System.out.println("DEBUG: Found " + strCount + " string attributes");
            
            // Create array for string sizes
            System.out.println("DEBUG: Creating string sizes array...");
            short[] strSizes = new short[strCount];
            // We'll use a default size for strings
            for (int i = 0; i < strCount; i++) {
                strSizes[i] = 32; // default string length
                System.out.println("DEBUG: String attribute " + (i+1) + " set to size 32");
            }
            
            // Create a heap file for the relation
            System.out.println("DEBUG: Creating heap file for relation: " + relationName);
            System.out.println("DEBUG: Current working directory: " + new File(".").getAbsolutePath());
            try {
                System.out.println("DEBUG: About to call Heapfile constructor");
                heapFile = new Heapfile(relationName);
                System.out.println("DEBUG: Heap file created successfully");
            } catch (Exception e) {
                System.err.println("CRITICAL ERROR in heap file creation: " + e);
                e.printStackTrace();
                throw e;
            }
            
            // Create attribute information array for catalog
            System.out.println("DEBUG: Creating attribute info for catalog...");
            attrInfo[] attrInfo = new attrInfo[attrCount];
            
            for (int i = 0; i < attrCount; i++) {
                System.out.println("DEBUG: Setting up attribute info " + (i+1) + "...");
                attrInfo[i] = new attrInfo();
                attrInfo[i].attrName = "attr" + i;
                attrInfo[i].attrType = attrTypes[i]; 
                
                if (attrTypes[i].attrType == AttrType.attrString) {
                    attrInfo[i].attrLen = 32; // Default string length
                    System.out.println("DEBUG: Attribute " + (i+1) + " (STRING) length set to 32");
                } else if (attrTypes[i].attrType == AttrType.attrInteger) {
                    attrInfo[i].attrLen = 4;
                    System.out.println("DEBUG: Attribute " + (i+1) + " (INTEGER) length set to 4");
                } else if (attrTypes[i].attrType == AttrType.attrReal) {
                    attrInfo[i].attrLen = 4;
                    System.out.println("DEBUG: Attribute " + (i+1) + " (REAL) length set to 4");
                } else if (attrTypes[i].attrType == AttrType.attrVector100D) {
                    attrInfo[i].attrLen = 400; // 100 integers * 4 bytes
                    System.out.println("DEBUG: Attribute " + (i+1) + " (VECTOR100D) length set to 400");
                }
            }
            
            // Add relation to catalog
            System.out.println("DEBUG: Adding relation to catalog: " + relationName);
            try {
                // Use the public createRel method from RelCatalog via ExtendedSystemDefs
                ExtendedSystemDefs.MINIBASE_RELCAT.createRel(relationName, attrCount, attrInfo);
                System.out.println("DEBUG: Relation added to catalog successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to create relation in catalog: " + e.getMessage());
                throw new Exception("Failed to create relation in catalog: " + e.getMessage());
            }
            
            // Read tuples from the data file and insert them
            System.out.println("DEBUG: Starting to read and insert tuples...");
            String line;
            int tupleCount = 0;
            
            while (true) {
                System.out.println("DEBUG: --- Processing tuple #" + (tupleCount + 1) + " ---");
                
                // Create a new tuple for insertion
                System.out.println("DEBUG: Creating new tuple template...");
                Tuple tuple = new Tuple();
                
                // Setup a tuple template
                System.out.println("DEBUG: Setting tuple header with " + attrCount + " attributes");
                tuple.setHdr((short) attrCount, attrTypes, strSizes);
                
                int tupleSize = tuple.size();
                System.out.println("DEBUG: Tuple size calculated as " + tupleSize + " bytes");
                
                byte[] tupleData = new byte[tupleSize];
                System.out.println("DEBUG: Created byte array for tuple data");
                
                tuple = new Tuple(tupleData, 0, tupleSize);
                System.out.println("DEBUG: Created new tuple instance with byte array");
                
                tuple.setHdr((short) attrCount, attrTypes, strSizes);
                System.out.println("DEBUG: Reset tuple header");
                
                boolean endOfFile = false;
                
                // Read a tuple worth of data
                for (int i = 0; i < attrCount; i++) {
                    System.out.println("DEBUG: Reading attribute " + (i+1) + " of type " + attrTypes[i].attrType);
                    
                    line = reader.readLine();
                    
                    if (line == null) {
                        System.out.println("DEBUG: End of file reached while reading attribute " + (i+1));
                        endOfFile = true;
                        break;
                    }
                    
                    System.out.println("DEBUG: Raw line read: '" + line + "'");
                    line = line.trim();
                    System.out.println("DEBUG: Trimmed line: '" + line + "'");
                    
                    try {
                        switch (attrTypes[i].attrType) {
                            case AttrType.attrInteger:
                                int intVal = Integer.parseInt(line);
                                tuple.setIntFld(i+1, intVal);
                                System.out.println("DEBUG: Set integer field " + (i+1) + " = " + intVal);
                                break;
                                
                            case AttrType.attrReal:
                                float floatVal = Float.parseFloat(line);
                                tuple.setFloFld(i+1, floatVal);
                                System.out.println("DEBUG: Set float field " + (i+1) + " = " + floatVal);
                                break;
                                
                            case AttrType.attrString:
                                tuple.setStrFld(i+1, line);
                                System.out.println("DEBUG: Set string field " + (i+1) + " = '" + line + "'");
                                break;
                                
                            case AttrType.attrVector100D:
                                // Parse a line of 100 numbers for the vector
                                System.out.println("DEBUG: Parsing vector data from line: '" +
                                                   (line.length() > 50 ? line.substring(0, 50) + "..." : line) + "'");

                                String[] vectorVals = line.split("\\s+");
                                System.out.println("DEBUG: Split vector into " + vectorVals.length + " values");

                                if (vectorVals.length != 100) {
                                    System.out.println("ERROR: Vector must have exactly 100 values, found " +
                                                     vectorVals.length);
                                    throw new Exception("Vector must have exactly 100 values, found " + vectorVals.length);
                                }

                                int[] vector = new int[100];
                                for (int j = 0; j < 100; j++) {
                                    try {
                                        // *** FIX: Parse as float first, then cast to int ***
                                        float floatVal2 = Float.parseFloat(vectorVals[j]);
                                        vector[j] = (int) floatVal2; // Cast to integer

                                        // Check range constraint (-10000 to 10000) AFTER casting
                                        if (vector[j] < -10000 || vector[j] > 10000) {
                                            System.out.println("ERROR: Vector value (after casting) out of range: " + vector[j]);
                                            throw new Exception("Vector values must be in range -10000 to 10000");
                                        }
                                    } catch (NumberFormatException e) {
                                        // This will catch errors parsing the float itself
                                        System.out.println("ERROR: Invalid numeric vector value at position " + j +
                                                         ": '" + vectorVals[j] + "'");
                                        // Re-throw with more context
                                        throw new NumberFormatException("Invalid numeric vector value at position " + j + ": '" + vectorVals[j] + "'");
                                    }
                                }

                                tuple.setVectorFld(i+1, vector);
                                System.out.println("DEBUG: Set vector field " + (i+1) +
                                                 " (first 3 values: " + vector[0] + "," + vector[1] + "," + vector[2] + "...)");
                                break;
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR parsing attribute " + (i+1) + ": " + e.getMessage());
                        throw new Exception("Error parsing attribute " + (i+1) + ": " + e.getMessage());
                    }
                }
                
                // If we reached end of file before completing a tuple, break the loop
                if (endOfFile) {
                    System.out.println("DEBUG: End of file reached, stopping tuple insertion");
                    break;
                }
                
                // Insert the tuple into the heap file
                System.out.println("DEBUG: Inserting tuple into heap file...");
                try {
                    RID rid = heapFile.insertRecord(tuple.getTupleByteArray());
                    System.out.println("DEBUG: Tuple inserted successfully, RID: [" + 
                                     rid.pageNo + "," + rid.slotNo + "]");
                    tupleCount++;
                    System.out.println("DEBUG: Tuple count now: " + tupleCount);
                } catch (Exception e) {
                    System.err.println("ERROR inserting tuple: " + e.getMessage());
                    throw e;
                }
            }
            
            // Report success and statistics
            System.out.println("Successfully created table '" + relationName + "' with " + tupleCount + " tuples.");
            System.out.println("Page reads: " + PCounter.getRCount());
            System.out.println("Page writes: " + PCounter.getWCount());
            
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
            e.printStackTrace();
            
            // Clean up resources if an error occurred
            if (heapFile != null) {
                try {
                    System.out.println("DEBUG: Attempting to delete heap file due to error");
                    heapFile.deleteFile();
                    System.out.println("DEBUG: Heap file deleted successfully");
                } catch (Exception ex) {
                    System.err.println("Error deleting heap file during cleanup: " + ex.getMessage());
                }
            }
            
            throw e;
        } finally {
            // Close resources
            if (reader != null) {
                System.out.println("DEBUG: Closing file reader");
                reader.close();
            }
        }
    }


    private static void createIndex(String relationName, int columnId, int lValue, int hValue) throws Exception {
        if (!dbOpen) {
            System.out.println("Error: No database is open. Please open a database first.");
            return;
        }

        System.out.println("Creating index on " + relationName + " column " + columnId + "...");
        PCounter.initialize(); // Reset counters for this operation

        Heapfile heapFile = null;
        Scan scan = null;
        IndexDesc checkIndexDesc = new IndexDesc(); // For checking existence
        boolean proceed = false;

        try {
            // --- 1. Get Relation and Attribute Info ---
            RelDesc relDesc = new RelDesc();
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relationName, relDesc);

            if (columnId < 1 || columnId > relDesc.attrCnt) {
                throw new Exception("Column ID " + columnId + " is out of range (1-" + relDesc.attrCnt + ")");
            }

            AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
            for (int i = 0; i < relDesc.attrCnt; i++) attrDescs[i] = new AttrDesc();
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relationName, relDesc.attrCnt, attrDescs);

            AttrDesc attrDesc = attrDescs[columnId - 1];
            AttrType attrType = attrDesc.attrType;
            String attrName = attrDesc.attrName;
            IndexType indexTypeToCreate; // Determine the type we intend to create

            // Determine intended index type based on attribute type
            if (attrType.attrType == AttrType.attrVector100D) {
                indexTypeToCreate = new IndexType(IndexType.LSHFIndex);
            } else if (attrType.attrType == AttrType.attrInteger || attrType.attrType == AttrType.attrReal || attrType.attrType == AttrType.attrString) {
                indexTypeToCreate = new IndexType(IndexType.B_Index);
            } else {
                throw new Catalogbadtype(null, "Cannot create index on attribute type: " + attrType);
            }

            // --- 2. Check if Index Already Exists in Catalog ---
            System.out.println("DEBUG: Checking if index already exists for " + relationName + "." + attrName);
            try {
                // Use the corrected getInfo method
                ExtendedSystemDefs.MINIBASE_INDCAT.getInfo(relationName, attrName, indexTypeToCreate, checkIndexDesc);

                // If getInfo succeeded, the index already exists!
                System.out.println("DEBUG: Index already exists.");
                throw new Catalogindexexists(null, "Index already exists on " + relationName + "." + attrName);

            } catch (Catalogindexnotfound e_notfound) {
                // *** This is the expected case - index does NOT exist, okay to proceed ***
                System.out.println("DEBUG: Index does not exist. Proceeding with creation.");
                proceed = true;
            }
            // Let other exceptions from getInfo (like Catalogioerror) propagate up

            if (!proceed) {
                 // Should not happen if logic is correct, but as a safeguard
                 throw new Exception("Index existence check failed unexpectedly.");
            }

            // --- 3. Create and Populate the Physical Index ---
            heapFile = new Heapfile(relationName);
            String indexFileName = null; // Store the created index file name
            int indexedTupleCount = 0;

            // Prepare schema info for reading tuples
            AttrType[] tupleAttrTypes = new AttrType[relDesc.attrCnt];
            int strCount = 0;
            for (int i = 0; i < relDesc.attrCnt; i++) {
                tupleAttrTypes[i] = attrDescs[i].attrType;
                if (tupleAttrTypes[i].attrType == AttrType.attrString) strCount++;
            }
            short[] tupleStrSizes = new short[strCount];
            int strIndex = 0;
            for (int i = 0; i < relDesc.attrCnt; i++) {
                if (tupleAttrTypes[i].attrType == AttrType.attrString) {
                    tupleStrSizes[strIndex++] = (short) attrDescs[i].attrLen;
                }
            }

            // --- LSH Index Creation ---
            if (indexTypeToCreate.indexType == IndexType.LSHFIndex) {
                System.out.println("Creating LSH index with " + lValue + " layers and " + hValue + " hash functions per layer...");
                LSHFIndex lshIndex = new LSHFIndex(hValue, lValue);

                scan = heapFile.openScan();
                RID rid = new RID();
                Tuple tuple = null;

                while ((tuple = scan.getNext(rid)) != null) {
                    RID ridCopy = new RID(rid.pageNo, rid.slotNo);
                    tuple.setHdr((short) tupleAttrTypes.length, tupleAttrTypes, tupleStrSizes);
                    try {
                        int[] vectorData = tuple.getVectorFld(columnId);
                        Vector100Dtype vectorObj = new Vector100Dtype(vectorData);
                        Vector100DKey key = new Vector100DKey(vectorObj);
                        lshIndex.insert(key, ridCopy);
                        indexedTupleCount++;
                    } catch (Exception e) {
                        System.err.println("Warning: Could not index tuple for LSH: " + e.getMessage());
                    }
                }
                scan.closescan();
                scan = null;

                // Build index file name and save
                indexFileName = relationName + columnId + "_L" + lValue + "_h" + hValue + ".ser";
                lshIndex.saveIndex(indexFileName);
                System.out.println("LSH index structure saved to file: " + indexFileName);

            // --- B-Tree Index Creation ---
        } else if (indexTypeToCreate.indexType == IndexType.B_Index) {
            // Build B-Tree index file name (using catalog helper is good practice)
            indexFileName = ExtendedSystemDefs.MINIBASE_INDCAT.buildIndexName(relationName, attrName, indexTypeToCreate);
            System.out.println("Creating B-Tree index file: " + indexFileName);
            BTreeFile btreeFile = null;

            try {
                // --- MODIFICATION START ---
                int btreeKeySize;
                int btreeKeyType; // Declare here, determine below

                if (attrType.attrType == AttrType.attrReal) {
                    // Since data is whole numbers, cast Real to Integer for the key:
                    btreeKeyType = AttrType.attrInteger; // Tell BTreeFile to expect Integer keys
                    btreeKeySize = 4; // Standard integer size
                } else {
                    // For other types, use their natural type and length
                    btreeKeyType = attrType.attrType;
                    btreeKeySize = attrDesc.attrLen;
                }
                // --- MODIFICATION END ---

                // Create the BTreeFile using the determined type and size
                btreeFile = new BTreeFile(indexFileName, btreeKeyType, btreeKeySize, 0); // 0 for non-delete


                // Scan data and insert into BTree
                scan = heapFile.openScan();
                RID rid = new RID();
                Tuple tuple = null;

                while ((tuple = scan.getNext(rid)) != null) {
                    // Make a copy of RID
                    RID ridCopy = new RID(rid.pageNo, rid.slotNo);
                    // Set header
                    tuple.setHdr((short) tupleAttrTypes.length, tupleAttrTypes, tupleStrSizes);

                    KeyClass key = null;
                    try {
                        // Extract key based on type
                        switch (attrType.attrType) { // Still switch on the original attribute type
                            case AttrType.attrInteger:
                                key = new IntegerKey(tuple.getIntFld(columnId));
                                break;
                            case AttrType.attrReal:
                                // Casting float (which is a whole number) to IntegerKey
                                key = new IntegerKey((int)tuple.getFloFld(columnId)); // <-- CAST HERE
                                break;
                            case AttrType.attrString:
                                key = new StringKey(tuple.getStrFld(columnId));
                                break;
                        }

                        // Insert into BTree
                        if (key != null) {
                            btreeFile.insert(key, ridCopy); // Use the IntegerKey
                            indexedTupleCount++;
                        }
                    } catch (Exception e) { // Catch potential duplicate key exceptions here if BTree throws them
                         System.err.println("Warning: Could not insert key for RID [" + ridCopy.pageNo + "," + ridCopy.slotNo + "] into BTree: " + e.getMessage());
                         e.printStackTrace();
                         // Optionally re-throw if insertion failure should stop the whole process
                    }
                } // End while scan
                scan.closescan();
                scan = null;

            }  finally {
                // Ensure BTree file is closed if created
                if (btreeFile != null) {
                    try { btreeFile.close(); } catch (Exception e_close) { /* Ignore */ }
                }
                // Ensure scan is closed if open
                if (scan != null) {
                    try { scan.closescan(); } catch (Exception e_scan) { /* Ignore */ }
                }
            }
             System.out.println("B-Tree index populated.");
        } // End B-Tree creation

            // --- 4. Add Entry to Index Catalog (AFTER physical index is created) ---
            System.out.println("DEBUG: Adding index entry to catalog...");
            IndexDesc indexDesc = new IndexDesc();
            indexDesc.relName = relationName;
            indexDesc.attrName = attrName;
            indexDesc.accessType = indexTypeToCreate; // Use the determined type
            indexDesc.order = (indexTypeToCreate.indexType == IndexType.B_Index) ?
                              new TupleOrder(TupleOrder.Ascending) : new TupleOrder(TupleOrder.Random);
            indexDesc.clustered = 0; // Assuming non-clustered
            indexDesc.distinctKeys = 0; // Placeholder - could estimate later
            indexDesc.indexPages = 0;   // Placeholder - could estimate later

            if (ExtendedSystemDefs.MINIBASE_INDCAT == null) {
                System.err.println("CRITICAL ERROR: ExtendedSystemDefs.MINIBASE_INDCAT is NULL before calling addIndexCatalogEntry!");
                throw new NullPointerException("MINIBASE_INDCAT is null. Catalog initialization likely failed.");
            }


            // Use the new simplified method
            ExtendedSystemDefs.MINIBASE_INDCAT.addIndexCatalogEntry(indexDesc);
            System.out.println("DEBUG: Index entry added to catalog.");

            // --- 5. Update Attribute Catalog Index Count ---
            System.out.println("DEBUG: Updating attribute catalog index count...");
            // Need to read the attribute record again to modify it safely
            AttrDesc attrToUpdate = new AttrDesc();
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getInfo(relationName, attrName, attrToUpdate);
            attrToUpdate.indexCnt++; // Increment index count
            // Remove old entry and add updated entry (or use an update method if available)
            ExtendedSystemDefs.MINIBASE_ATTRCAT.removeInfo(relationName, attrName);
            ExtendedSystemDefs.MINIBASE_ATTRCAT.addInfo(attrToUpdate);
            System.out.println("DEBUG: Attribute catalog updated.");


            // --- 6. Report Success ---
            if (indexTypeToCreate.indexType == IndexType.LSHFIndex) {
                 System.out.println("Created LSH index on " + relationName + "." + attrName +
                                  " with " + indexedTupleCount + " entries.");
                 System.out.println("Index structure saved to file: " + indexFileName);
            } else {
                 System.out.println("Created B-Tree index on " + relationName + "." + attrName +
                                  " with " + indexedTupleCount + " entries.");
                 System.out.println("Index file created: " + indexFileName);
            }

            System.out.println("Index creation complete.");
            System.out.println("Disk pages read: " + PCounter.getRCount());
            System.out.println("Disk pages written: " + PCounter.getWCount());

        } catch (Exception e) {
            // Close resources if still open
            if (scan != null) {
                try { scan.closescan(); } catch (Exception ex) { /* Ignore cleanup errors */ }
            }
            // Consider deleting partially created index file if appropriate
            System.err.println("Error during index creation: " + e.getMessage());
            e.printStackTrace(); // Print stack trace for debugging
            // Re-throw the exception to signal failure
            throw e;
        }
    }


    private static void batchInsert(String updateFileName, String relationName) throws Exception {
        if (!dbOpen) {
            System.out.println("Error: No database is open. Please open a database first.");
            return;
        }
        
        System.out.println("Batch inserting data into table '" + relationName + "' from file " + updateFileName + "...");
        PCounter.initialize(); // Reset counters for this operation
        
        BufferedReader reader = null;
        Heapfile heapFile = null;
        
        try {
            // Check if relation exists & get relation descriptor
            RelDesc relDesc = new RelDesc();
            try {
                ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relationName, relDesc);
            } catch (Exception e) {
                throw new Exception("Relation " + relationName + " does not exist");
            }
            
            // Open update file
            reader = new BufferedReader(new FileReader(updateFileName));
            
            // Read the number of attributes
            int attrCount = Integer.parseInt(reader.readLine().trim());
            if (attrCount != relDesc.attrCnt) {
                throw new Exception("Attribute count mismatch: update file has " + attrCount + 
                                    " attributes but relation has " + relDesc.attrCnt);
            }
            
            // Read the attribute types
            String[] typeStrs = reader.readLine().trim().split("\\s+");
            if (typeStrs.length != attrCount) {
                throw new Exception("Mismatch between attribute count and type count in update file");
            }
            
            // Get attribute descriptors from the catalog
            AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
            for (int i = 0; i < relDesc.attrCnt; i++) {
                attrDescs[i] = new AttrDesc();
            }
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relationName, relDesc.attrCnt, attrDescs);
            
            // Prepare schema information
            AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
            int strCount = 0;
            for (int i = 0; i < relDesc.attrCnt; i++) {
                attrTypes[i] = attrDescs[i].attrType;
                if (attrTypes[i].attrType == AttrType.attrString) {
                    strCount++;
                }
                
                // Verify that provided types match the schema
                int fileType = Integer.parseInt(typeStrs[i]);
                int expectedType;
                switch (attrTypes[i].attrType) {
                    case AttrType.attrInteger: expectedType = 1; break;
                    case AttrType.attrReal: expectedType = 2; break;
                    case AttrType.attrString: expectedType = 3; break;
                    case AttrType.attrVector100D: expectedType = 4; break;
                    default: expectedType = -1;
                }
                
                if (fileType != expectedType) {
                    throw new Exception("Type mismatch for attribute " + (i+1) + ": expected " + expectedType + " but got " + fileType);
                }
            }
            
            // Create string sizes array
            short[] strSizes = new short[strCount];
            int strIndex = 0;
            for (int i = 0; i < relDesc.attrCnt; i++) {
                if (attrTypes[i].attrType == AttrType.attrString) {
                    strSizes[strIndex++] = (short) attrDescs[i].attrLen;
                }
            }
            
            // Get indexes for this relation using the correct approach:
            // 1. First get the index count from the relation descriptor
            int indexCount = relDesc.indexCnt;
            
            // 2. Create and populate index descriptors if any indexes exist
            IndexDesc[] indexDescs = null;
            if (indexCount > 0) {
                indexDescs = new IndexDesc[indexCount];
                for (int i = 0; i < indexCount; i++) {
                    indexDescs[i] = new IndexDesc();
                }
                try {
                    // Use proper catalog method to get index information
                    ExtendedSystemDefs.MINIBASE_INDCAT.getRelInfo(relationName, indexCount, indexDescs);
                } catch (Exception e) {
                    System.err.println("Warning: Failed to retrieve index information: " + e.getMessage());
                    indexCount = 0;
                    indexDescs = null;
                }
            }
            
            // Open heap file
            heapFile = new Heapfile(relationName);
            
            // Read and insert tuples
            String line;
            int insertedCount = 0;
            
            while (true) {
                // Create a new tuple for insertion
                Tuple tuple = new Tuple();
                tuple.setHdr((short) attrCount, attrTypes, strSizes);
                int tupleSize = tuple.size();
                byte[] tupleData = new byte[tupleSize];
                tuple = new Tuple(tupleData, 0, tupleSize);
                tuple.setHdr((short) attrCount, attrTypes, strSizes);
                
                boolean endOfFile = false;
                
                // Read a tuple worth of data
                for (int i = 0; i < attrCount; i++) {
                    line = reader.readLine();
                    if (line == null) {
                        endOfFile = true;
                        break;
                    }
                    
                    line = line.trim();
                    
                    try {
                        switch (attrTypes[i].attrType) {
                            case AttrType.attrInteger:
                                int intVal = Integer.parseInt(line);
                                tuple.setIntFld(i+1, intVal);
                                break;
                                
                            case AttrType.attrReal:
                                float floatVal = Float.parseFloat(line);
                                tuple.setFloFld(i+1, floatVal);
                                break;
                                
                            case AttrType.attrString:
                                tuple.setStrFld(i+1, line);
                                break;
                                
                            case AttrType.attrVector100D:
                                // Parse a line of 100 integers for the vector
                                String[] vectorVals = line.split("\\s+");
                                if (vectorVals.length != 100) {
                                    throw new Exception("Vector must have exactly 100 values");
                                }
                                
                                int[] vector = new int[100];
                                for (int j = 0; j < 100; j++) {
                                    int val = Integer.parseInt(vectorVals[j]);
                                    // Check range constraint (-10000 to 10000)
                                    if (val < -10000 || val > 10000) {
                                        throw new Exception("Vector values must be in range -10000 to 10000");
                                    }
                                    vector[j] = val;
                                }
                                
                                tuple.setVectorFld(i+1, vector);
                                break;
                        }
                    } catch (Exception e) {
                        throw new Exception("Error parsing attribute " + (i+1) + ": " + e.getMessage());
                    }
                }
                
                // If we reached end of file before completing a tuple, break the loop
                if (endOfFile) {
                    break;
                }
                
                // Insert the tuple into the heap file
                RID rid = heapFile.insertRecord(tuple.getTupleByteArray());
                insertedCount++;
                
                // Update all indexes
                if (indexDescs != null && indexCount > 0) {
                    for (int i = 0; i < indexCount; i++) {
                        IndexDesc indexDesc = indexDescs[i];
                        
                        // Find the attribute number for this index
                        int indexAttrPos = -1;
                        for (int j = 0; j < relDesc.attrCnt; j++) {
                            if (attrDescs[j].attrName.equals(indexDesc.attrName)) {
                                indexAttrPos = j + 1; // Convert to 1-based position
                                break;
                            }
                        }
                        
                        if (indexAttrPos == -1) {
                            System.err.println("Warning: Could not find attribute for index: " + indexDesc.attrName);
                            continue;
                        }
                        
                        // Update the index based on its type
                        if (indexDesc.accessType.indexType == IndexType.B_Index) {
                            // BTree index
                            BTreeFile btf = null;
                            try {
                                // Format: relationName + columnId + ".btree"
                                // Note: We use indCat.buildIndexName() method to get the correct name
                                String btreeFileName = ExtendedSystemDefs.MINIBASE_INDCAT.buildIndexName(
                                    relationName, indexDesc.attrName, indexDesc.accessType);
                                    
                                btf = new BTreeFile(btreeFileName);
                                
                                // Extract key from tuple based on attribute type
                                KeyClass key = null;
                                switch (attrTypes[indexAttrPos-1].attrType) {
                                    case AttrType.attrInteger:
                                        key = new IntegerKey(tuple.getIntFld(indexAttrPos));
                                        break;
                                    case AttrType.attrReal:
                                        // Convert float to string for BTree
                                        key = new IntegerKey((int)tuple.getFloFld(indexAttrPos));
                                        break;
                                    case AttrType.attrString:
                                        key = new StringKey(tuple.getStrFld(indexAttrPos));
                                        break;
                                    default:
                                        throw new Exception("Unsupported attribute type for BTree index: " + 
                                                          attrTypes[indexAttrPos-1].attrType);
                                }
                                
                                // Insert into BTree
                                btf.insert(key, rid);
                            } catch (Exception e) {
                                System.err.println("Warning: Error updating BTree index: " + e.getMessage());
                            } finally {
                                // Close BTree file if opened
                                if (btf != null) {
                                    try { btf.close(); } catch (Exception e) { /* Ignore */ }
                                }
                            }
                        } else if (indexDesc.accessType.indexType == IndexType.LSHFIndex) {
                            try {
                                // Find the LSH index files that match this relation and column
                                // Format from createIndex: relationName + "." + columnId + "." + lValue + "." + hValue + ".ser"
                                File dir = new File(".");
                                // **FIX: Correct the prefix to match the createIndex naming convention**
                                // The prefix should be "RELNAME.COLUMNID." to find files like "REL1.2.10.5.ser"
                                String prefix = relationName + "." + indexAttrPos + ".";
                                File[] matchingFiles = dir.listFiles((d, name) ->
                                    name.startsWith(prefix) && name.endsWith(".ser"));

                                if (matchingFiles != null && matchingFiles.length > 0) {
                                    for (File indexFile : matchingFiles) {
                                        String indexFileName = indexFile.getName();

                                        // Load the LSH index
                                        LSHFIndex lshIndex = LSHFIndex.loadIndex(indexFileName);

                                        // Extract vector data from tuple
                                        int[] vectorData = tuple.getVectorFld(indexAttrPos);
                                        Vector100Dtype vectorObj = new Vector100Dtype(vectorData);
                                        Vector100DKey key = new Vector100DKey(vectorObj);

                                        // Insert into LSH index
                                        lshIndex.insert(key, new RID(rid.pageNo, rid.slotNo));

                                        // Save the updated index
                                        lshIndex.saveIndex(indexFileName);
                                    }
                                } else {
                                     // Optional: Add a warning if no index file was found for an expected index
                                     System.err.println("Warning: No LSH index file found matching prefix '" + prefix + "' for relation '" + relationName + "' attribute " + indexAttrPos);
                                }
                            } catch (Exception e) {
                                System.err.println("Warning: Error updating LSH index: " + e.getMessage());
                            }
                        }
                    }
                }
            }
            
            // Report success and statistics
            System.out.println("Successfully inserted " + insertedCount + " tuples into relation '" + relationName + "'.");
            System.out.println("Page reads: " + PCounter.getRCount());
            System.out.println("Page writes: " + PCounter.getWCount());
            
        } catch (Exception e) {
            System.err.println("Error batch inserting data: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            // Close resources
            if (reader != null) {
                try { reader.close(); } catch (Exception e) { /* Ignore */ }
            }
        }
    }

/**
 * Delete data from an existing table based on conditions in a file.
 * Also updates all indexes associated with the relation.
 * 
 * @param updateFileName Name of the file containing deletion conditions
 * @param relationName Name of the relation to delete data from
 * @throws Exception if there's an error during deletion
 */
private static void batchDelete(String updateFileName, String relationName) throws Exception {
    if (!dbOpen) {
        System.out.println("Error: No database is open. Please open a database first.");
        return;
    }
    
    System.out.println("Batch deleting data from table '" + relationName + "' using file " + updateFileName + "...");
    PCounter.initialize(); // Reset counters for this operation
    
    BufferedReader reader = null;
    Heapfile heapFile = null;
    Scan scan = null;
    
    try {
        // Check if relation exists & get relation descriptor
        RelDesc relDesc = new RelDesc();
        try {
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relationName, relDesc);
        } catch (Exception e) {
            throw new Exception("Relation " + relationName + " does not exist");
        }
        
        // Open the update file
        reader = new BufferedReader(new FileReader(updateFileName));
        
        // Read the number of attributes
        int attrCount = Integer.parseInt(reader.readLine().trim());
        if (attrCount != relDesc.attrCnt) {
            throw new Exception("Attribute count mismatch: update file has " + attrCount + 
                                " attributes but relation has " + relDesc.attrCnt);
        }
        
        // Read the attribute types
        String[] typeStrs = reader.readLine().trim().split("\\s+");
        if (typeStrs.length != attrCount) {
            throw new Exception("Mismatch between attribute count and type count in update file");
        }
        
        // Get attribute descriptors from the catalog
        AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
        for (int i = 0; i < relDesc.attrCnt; i++) {
            attrDescs[i] = new AttrDesc();
        }
        ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relationName, relDesc.attrCnt, attrDescs);
        
        // Prepare schema information
        AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
        int strCount = 0;
        for (int i = 0; i < relDesc.attrCnt; i++) {
            attrTypes[i] = attrDescs[i].attrType;
            if (attrTypes[i].attrType == AttrType.attrString) {
                strCount++;
            }
            
            // Verify that provided types match the schema
            int fileType = Integer.parseInt(typeStrs[i]);
            int expectedType;
            switch (attrTypes[i].attrType) {
                case AttrType.attrInteger: expectedType = 1; break;
                case AttrType.attrReal: expectedType = 2; break;
                case AttrType.attrString: expectedType = 3; break;
                case AttrType.attrVector100D: expectedType = 4; break;
                default: expectedType = -1;
            }
            
            if (fileType != expectedType) {
                throw new Exception("Type mismatch for attribute " + (i+1) + ": expected " + expectedType + " but got " + fileType);
            }
        }
        
        // Create string sizes array
        short[] strSizes = new short[strCount];
        int strIndex = 0;
        for (int i = 0; i < relDesc.attrCnt; i++) {
            if (attrTypes[i].attrType == AttrType.attrString) {
                strSizes[strIndex++] = (short) attrDescs[i].attrLen;
            }
        }
        
        // Get indexes for this relation
        int indexCount = relDesc.indexCnt;
        IndexDesc[] indexDescs = null;
        if (indexCount > 0) {
            indexDescs = new IndexDesc[indexCount];
            for (int i = 0; i < indexCount; i++) {
                indexDescs[i] = new IndexDesc();
            }
            try {
                ExtendedSystemDefs.MINIBASE_INDCAT.getRelInfo(relationName, indexCount, indexDescs);
            } catch (Exception e) {
                System.err.println("Warning: Failed to retrieve index information: " + e.getMessage());
                indexCount = 0;
                indexDescs = null;
            }
        }
        
        // Open heap file
        heapFile = new Heapfile(relationName);
        
        // Read deletion conditions from the file
        List<DeletionCondition> conditions = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            
            String[] parts = line.split("\\s+", 2); // Split into two parts: attr_num and value
            if (parts.length < 2) {
                throw new Exception("Invalid deletion condition format: " + line);
            }
            
            int attrNum = Integer.parseInt(parts[0]);
            if (attrNum < 1 || attrNum > attrCount) {
                throw new Exception("Invalid attribute number in deletion condition: " + attrNum);
            }
            
            // Convert to 0-based for internal use
            int attrPos = attrNum - 1;
            
            // Create appropriate condition value based on attribute type
            DeletionCondition condition = new DeletionCondition();
            condition.attrNum = attrNum;
            
            switch (attrTypes[attrPos].attrType) {
                case AttrType.attrInteger:
                    condition.type = DeletionCondition.TYPE_INT;
                    condition.intValue = Integer.parseInt(parts[1]);
                    break;
                    
                case AttrType.attrReal:
                    condition.type = DeletionCondition.TYPE_FLOAT;
                    condition.floatValue = Float.parseFloat(parts[1]);
                    break;
                    
                case AttrType.attrString:
                    condition.type = DeletionCondition.TYPE_STRING;
                    condition.stringValue = parts[1];
                    break;
                    
                case AttrType.attrVector100D:
                    condition.type = DeletionCondition.TYPE_VECTOR;
                    String[] vectorValues = parts[1].split("\\s+");
                    if (vectorValues.length != 100) {
                        throw new Exception("Vector must have exactly 100 values");
                    }
                    
                    int[] vector = new int[100];
                    for (int i = 0; i < 100; i++) {
                        int val = Integer.parseInt(vectorValues[i]);
                        // Check range constraint (-10000 to 10000)
                        if (val < -10000 || val > 10000) {
                            throw new Exception("Vector values must be in range -10000 to 10000");
                        }
                        vector[i] = val;
                    }
                    condition.vectorValue = vector;
                    break;
                    
                default:
                    throw new Exception("Unsupported attribute type for deletion condition");
            }
            
            conditions.add(condition);
        }
        
        // If no conditions were found, warn and exit
        if (conditions.isEmpty()) {
            System.out.println("Warning: No deletion conditions found in file. No records will be deleted.");
            return;
        }
        
        // Scan the heap file for matching records to delete
        scan = heapFile.openScan();
        RID rid = new RID();
        Tuple tuple = null;
        int deletedCount = 0;
        List<RID> toDelete = new ArrayList<>();
        Map<RID, Map<Integer, Object>> keysToDelete = new HashMap<>();
        
        // First scan: Find all records that match the conditions and prepare for deletion
        while ((tuple = scan.getNext(rid)) != null) {
            // Deep copy the RID since it's reused by scan.getNext()
            RID ridCopy = new RID(rid.pageNo, rid.slotNo);
            
            // Set header to properly interpret tuple data
            tuple.setHdr((short)attrTypes.length, attrTypes, strSizes);
            
            // Check if this tuple matches any deletion condition
            for (DeletionCondition condition : conditions) {
                boolean matches = false;
                
                try {
                    switch (condition.type) {
                        case DeletionCondition.TYPE_INT:
                            matches = (tuple.getIntFld(condition.attrNum) == condition.intValue);
                            break;
                            
                        case DeletionCondition.TYPE_FLOAT:
                            matches = (tuple.getFloFld(condition.attrNum) == condition.floatValue);
                            break;
                            
                        case DeletionCondition.TYPE_STRING:
                            matches = tuple.getStrFld(condition.attrNum).equals(condition.stringValue);
                            break;
                            
                        case DeletionCondition.TYPE_VECTOR:
                            int[] tupleVector = tuple.getVectorFld(condition.attrNum);
                            matches = Arrays.equals(tupleVector, condition.vectorValue);
                            break;
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Error checking condition on tuple: " + e.getMessage());
                    continue;
                }
                
                if (matches) {
                    // If we have indexes, store the key values for index updates
                    if (indexDescs != null && indexCount > 0) {
                        Map<Integer, Object> keyValues = new HashMap<>();
                        
                        for (int i = 0; i < indexCount; i++) {
                            IndexDesc indexDesc = indexDescs[i];
                            
                            // Find the attribute position for this index
                            int indexAttrPos = -1;
                            for (int j = 0; j < relDesc.attrCnt; j++) {
                                if (attrDescs[j].attrName.equals(indexDesc.attrName)) {
                                    indexAttrPos = j + 1; // Convert to 1-based
                                    break;
                                }
                            }
                            
                            if (indexAttrPos == -1) continue;
                            
                            // Extract and store key value based on attribute type
                            try {
                                switch (attrTypes[indexAttrPos-1].attrType) {
                                    case AttrType.attrInteger:
                                        keyValues.put(indexAttrPos, tuple.getIntFld(indexAttrPos));
                                        break;
                                    case AttrType.attrReal:
                                        keyValues.put(indexAttrPos, tuple.getFloFld(indexAttrPos));
                                        break;
                                    case AttrType.attrString:
                                        keyValues.put(indexAttrPos, tuple.getStrFld(indexAttrPos));
                                        break;
                                    case AttrType.attrVector100D:
                                        keyValues.put(indexAttrPos, tuple.getVectorFld(indexAttrPos));
                                        break;
                                }
                            } catch (Exception e) {
                                System.err.println("Warning: Error extracting key value: " + e.getMessage());
                            }
                        }
                        
                        keysToDelete.put(ridCopy, keyValues);
                    }
                    
                    toDelete.add(ridCopy);
                    break; // Once a condition matches, we can stop checking
                }
            }
        }
        
        // Close the scan
        scan.closescan();
        scan = null;
        
        // Delete the matching records
        for (RID deleteRid : toDelete) {
            try {
                // Delete the record from the heap file
                heapFile.deleteRecord(deleteRid);
                deletedCount++;
                
                // Update indexes if needed
                if (indexDescs != null && indexCount > 0) {
                    Map<Integer, Object> keyValues = keysToDelete.get(deleteRid);
                    
                    for (int i = 0; i < indexCount; i++) {
                        IndexDesc indexDesc = indexDescs[i];
                        
                        // Find the attribute position for this index
                        int indexAttrPos = -1;
                        for (int j = 0; j < relDesc.attrCnt; j++) {
                            if (attrDescs[j].attrName.equals(indexDesc.attrName)) {
                                indexAttrPos = j + 1; // Convert to 1-based
                                break;
                            }
                        }
                        
                        if (indexAttrPos == -1) continue;
                        
                        Object keyValue = keyValues.get(indexAttrPos);
                        if (keyValue == null) continue;
                        
                        // Update index based on type
                        if (indexDesc.accessType.indexType == IndexType.B_Index) {
                            BTreeFile btf = null;
                            try {
                                // Get index name
                                String btreeFileName = ExtendedSystemDefs.MINIBASE_INDCAT.buildIndexName(
                                    relationName, indexDesc.attrName, indexDesc.accessType);
                                    
                                btf = new BTreeFile(btreeFileName);
                                
                                // Create key and delete from BTree
                                KeyClass key = null;
                                switch (attrTypes[indexAttrPos-1].attrType) {
                                    case AttrType.attrInteger:
                                        key = new IntegerKey((Integer)keyValue);
                                        break;
                                    case AttrType.attrReal:
                                        key = new IntegerKey((int)((Float)keyValue).floatValue());
                                        break;
                                    case AttrType.attrString:
                                        key = new StringKey((String)keyValue);
                                        break;
                                }
                                
                                if (key != null) {
                                    btf.Delete(key, deleteRid);
                                }
                            } catch (Exception e) {
                                System.err.println("Warning: Error updating BTree index: " + e.getMessage());
                            } finally {
                                if (btf != null) {
                                    try { btf.close(); } catch (Exception e) { /* Ignore */ }
                                }
                            }
                        } else if (indexDesc.accessType.indexType == IndexType.LSHFIndex) {
                            try {
                                // Find matching LSH index files
                                File dir = new File(".");
                                // **FIX: Correct the prefix to match the createIndex naming convention**
                                // The prefix should be "RELNAME.COLUMNID." to find files like "REL1.2.10.5.ser"
                                String prefix = relationName + "." + indexAttrPos + "."; // Corrected prefix
                                File[] matchingFiles = dir.listFiles((d, name) ->
                                    name.startsWith(prefix) && name.endsWith(".ser"));

                                if (matchingFiles != null && matchingFiles.length > 0) {
                                    for (File indexFile : matchingFiles) {
                                        String indexFileName = indexFile.getName();

                                        // Load the LSH index
                                        LSHFIndex lshIndex = LSHFIndex.loadIndex(indexFileName);

                                        // Create vector key and delete
                                        if (keyValue instanceof int[]) {
                                            int[] vectorData = (int[])keyValue;
                                            Vector100Dtype vectorObj = new Vector100Dtype(vectorData);
                                            Vector100DKey key = new Vector100DKey(vectorObj);

                                            // Remove from LSH index
                                            lshIndex.delete(key, deleteRid);

                                            // Save the updated index
                                            lshIndex.saveIndex(indexFileName);
                                        } else {
                                             // Add warning if key type is unexpected
                                             System.err.println("Warning: Expected int[] key for LSH index deletion, but got " + (keyValue != null ? keyValue.getClass().getName() : "null"));
                                        }
                                    }
                                } else {
                                     // Optional: Add a warning if no index file was found for an expected index
                                     System.err.println("Warning: No LSH index file found matching prefix '" + prefix + "' for relation '" + relationName + "' attribute " + indexAttrPos + " during delete.");
                                }
                            } catch (Exception e) {
                                System.err.println("Warning: Error updating LSH index during delete: " + e.getMessage());
                            }
                        }
                    }
                }
                
            } catch (Exception e) {
                System.err.println("Warning: Failed to delete record: " + e.getMessage());
            }
        }
        
        // Report success and statistics
        System.out.println("Successfully deleted " + deletedCount + " tuples from relation '" + relationName + "'.");
        System.out.println("Page reads: " + PCounter.getRCount());
        System.out.println("Page writes: " + PCounter.getWCount());
        
    } catch (Exception e) {
        System.err.println("Error batch deleting data: " + e.getMessage());
        e.printStackTrace();
        throw e;
    } finally {
        // Close resources
        if (reader != null) {
            try { reader.close(); } catch (Exception e) { /* Ignore */ }
        }
        if (scan != null) {
            try { scan.closescan(); } catch (Exception e) { /* Ignore */ }
        }
    }
}

/**
 * Helper class to represent a deletion condition from the update file
 */
private static class DeletionCondition {
    static final int TYPE_INT = 1;
    static final int TYPE_FLOAT = 2;
    static final int TYPE_STRING = 3;
    static final int TYPE_VECTOR = 4;
    
    int attrNum;     // 1-based attribute number
    int type;        // type of the condition value
    
    // Value storage (only one will be used based on type)
    int intValue;
    float floatValue;
    String stringValue;
    int[] vectorValue;
}

    
    /**
     * Print help information for all commands
     */
    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  open database DBNAME - Open a database (create if not exists)");
        System.out.println("  close database - Close the current database");
        System.out.println("  batchcreate DATAFILENAME RELNAME - Create a new table");
        System.out.println("  createindex RELNAME COLUMNID L h - Create an index on a column");
        System.out.println("  batchinsert UPDATEFILENAME RELNAME - Insert data into a table");
        System.out.println("  batchdelete UPDATEFILENAME RELNAME - Delete data from a table");
        System.out.println("  query RELNAME1 RELNAME2 QSNAME NUMBUF - Run a query");
        System.out.println("  help - Display this help message");
        System.out.println("  exit - Exit the database interface");
    }


private static void executeQuery(String relName1, String relName2, String querySpecFile, int bufferPages) throws Exception {
    if (!dbOpen) {
        System.out.println("Error: No database is open. Please open a database first.");
        return;
    }
    
    System.out.println("Executing query using specification in " + querySpecFile + "...");
    PCounter.initialize(); // Reset counters for this operation
    
    // Configure buffer manager to use the specified number of buffer pages
    if (bufferPages > 0) {
        System.out.println("Note: Using up to " + bufferPages + " buffer pages (limited by current buffer pool size)");
    } else {
        bufferPages = DEFAULT_BUFFER_PAGES; // Use default if invalid value provided
        System.out.println("Note: Using default " + bufferPages + " buffer pages.");
    }
    
    BufferedReader reader = null;
    
    try {
        // Read the query specification file
        reader = new BufferedReader(new FileReader(querySpecFile));
        String queryLine = reader.readLine();
        
        if (queryLine == null || queryLine.trim().isEmpty()) {
            throw new Exception("Query specification file is empty");
        }
        
        queryLine = queryLine.trim();
        
        // Parse the query to identify type and parameters
        if (queryLine.startsWith("Sort(")) {
            // Handle Sort query
            executeSortQuery(queryLine, relName1, bufferPages); // Pass bufferPages
        } else if (queryLine.startsWith("Filter(")) {
            // Handle Filter query
            executeFilterQuery(queryLine, relName1, bufferPages); // Pass bufferPages
        } else if (queryLine.startsWith("Range(")) {
            // Handle Range query
            executeRangeQuery(queryLine, relName1, bufferPages); // Pass bufferPages
        } else if (queryLine.startsWith("NN(")) {
            // Handle Nearest Neighbor query
            executeNNQuery(queryLine, relName1, bufferPages); // Pass bufferPages
        } else if (queryLine.startsWith("DJOIN(")) {
            // Handle Distance Join query
            executeDJoinQuery(queryLine, relName1, relName2, bufferPages); // Pass bufferPages
        } else {
            throw new Exception("Unknown query type: " + queryLine);
        }
        
        // Report I/O statistics after query execution
        System.out.println("Query execution completed.");
        System.out.println("Page reads: " + PCounter.getRCount());
        System.out.println("Page writes: " + PCounter.getWCount());
        
    } catch (Exception e) {
        System.err.println("Error executing query: " + e.getMessage());
        e.printStackTrace();
        throw e;
    } finally {
        // Close resources
        if (reader != null) {
            try { reader.close(); } catch (Exception e) { /* Ignore */ }
        }
    }
}

/**
 * Parse parameters from a query expression like "Query(param1, param2, ...)"
 * 
 * @param queryExpr The full query expression
 * @return Array of parameter strings
 * @throws Exception if there's an error parsing the query
 */
private static String[] parseQueryParams(String queryExpr) throws Exception {
    int openParen = queryExpr.indexOf('(');
    int closeParen = queryExpr.lastIndexOf(')');
    
    if (openParen < 0 || closeParen < 0 || closeParen <= openParen) {
        throw new Exception("Invalid query syntax: " + queryExpr);
    }
    
    String paramsStr = queryExpr.substring(openParen + 1, closeParen).trim();
    
    // Handle empty parameter list
    if (paramsStr.isEmpty()) {
        return new String[0];
    }
    
    // Split by commas, but handle nested queries
    List<String> params = new ArrayList<>();
    int level = 0;
    StringBuilder current = new StringBuilder();
    
    for (int i = 0; i < paramsStr.length(); i++) {
        char c = paramsStr.charAt(i);
        
        if (c == '(') {
            level++;
            current.append(c);
        } else if (c == ')') {
            level--;
            current.append(c);
        } else if (c == ',' && level == 0) {
            // Only split at top level commas
            params.add(current.toString().trim());
            current = new StringBuilder();
        } else {
            current.append(c);
        }
    }
    
    if (current.length() > 0) {
        params.add(current.toString().trim());
    }
    
    return params.toArray(new String[0]);
}


/**
 * Execute a Sort query on vector data.
 * Format: Sort(QA, T, D, ...)
 * 
 * @param queryExpr The full query expression
 * @param relName The name of the relation to query
 * @param bufferPages The number of buffer pages allocated for this query
 * @throws Exception if there's an error executing the query
 */
private static void executeSortQuery(String queryExpr, String relName, int bufferPages) throws Exception {
    String[] params = parseQueryParams(queryExpr);

    // **FIX: Interpret 3rd parameter as K (number of results)**
    if (params.length < 3) {
        throw new Exception("Sort query requires at least 3 parameters: QA, T, K, [output fields]"); // Changed D to K
    }

    // Parse parameters
    int queryAttrNum = Integer.parseInt(params[0].trim()); // QA
    String targetVectorFile = params[1].trim(); // T
    int k = Integer.parseInt(params[2].trim()); // K - Number of results
    if (k < 0) {
         // Allow k=0 to mean "return all sorted results" for potential flexibility,
         // but the primary use case implies k > 0.
         System.out.println("Warning: K is negative ("+ k +"). Interpreting as return all sorted results (k=0).");
         k = 0;
    } else if (k == 0) {
         System.out.println("Note: K=0 specified. Returning all sorted results.");
    }


    // Parse output fields specification
    String[] outputFieldsSpec = null;
    if (params.length > 3) { // Corrected index check
        String outputSpec = params[3].trim(); // Corrected index
        if (!outputSpec.equals("*")) {
            outputFieldsSpec = outputSpec.split("\\s+");
        }
    }

    // Read target vector from file
    int[] targetVector = readVectorFromFile(targetVectorFile);
    Vector100Dtype targetVec = new Vector100Dtype(targetVector);

    // Get relation information
    RelDesc relDesc = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName, relDesc);

    // Get attribute information
    AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrDescs[i] = new AttrDesc();
    }
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName, relDesc.attrCnt, attrDescs);

    // Verify query attribute is a vector
    if (queryAttrNum < 1 || queryAttrNum > relDesc.attrCnt) {
        throw new Exception("Invalid query attribute number: " + queryAttrNum);
    }
    AttrType queryAttrType = attrDescs[queryAttrNum-1].attrType;
    if (queryAttrType.attrType != AttrType.attrVector100D) {
        throw new Exception("Sort query attribute QA must be a vector (type 4)");
    }
    int sortFieldLength = 400; // Length of Vector100D (100 * 4 bytes)

    // Setup schema attributes for the full relation
    AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
    int strCount = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrTypes[i] = attrDescs[i].attrType;
        if (attrTypes[i].attrType == AttrType.attrString) {
            strCount++;
        }
    }
    short[] strSizes = new short[strCount];
    int strIndex = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        if (attrTypes[i].attrType == AttrType.attrString) {
            strSizes[strIndex++] = (short) attrDescs[i].attrLen;
        }
    }

    // --- Setup Projection ---
    // Projection for the final output
    FldSpec[] projlistOutput = createProjectionList(outputFieldsSpec, relDesc.attrCnt);
    int outFldCnt = projlistOutput.length;

    // Determine the schema of the final projected output
    AttrType[] outTypes = new AttrType[outFldCnt];
    List<Short> tempStrSizesList = new ArrayList<>();
    int currentStrSizeIndex = 0;
     for(int i=0; i<outFldCnt; i++){
         int fldNum = projlistOutput[i].offset; // 1-based field number
         outTypes[i] = attrTypes[fldNum-1];
         if(outTypes[i].attrType == AttrType.attrString){
             // Find the correct string size from the original schema's strSizes
             int originalStrIndex = 0;
             for(int attrIdx=0; attrIdx<fldNum-1; attrIdx++){ // Count string fields before this one
                 if(attrTypes[attrIdx].attrType == AttrType.attrString) originalStrIndex++;
             }
             if (originalStrIndex < strSizes.length) {
                 tempStrSizesList.add(strSizes[originalStrIndex]);
             } else {
                 // This should not happen if schema is consistent
                 throw new Exception("Error determining string size for projected field " + fldNum);
             }
         }
     }
     short[] outStrSizes = new short[tempStrSizesList.size()];
     for(int i=0; i<tempStrSizesList.size(); i++) outStrSizes[i] = tempStrSizesList.get(i);


    // Projection for the FileScan feeding into Sort. Must include the sort field.
    // Projecting all fields is simplest.
    FldSpec[] projScan = new FldSpec[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        projScan[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }

    // --- Create Iterators ---
    FileScan fileScan = null;
    iterator.Sort sortIterator = null;
    try {
        // 1. FileScan to read the base relation
        fileScan = new FileScan(relName, attrTypes, strSizes,
                                (short) relDesc.attrCnt, (short) projScan.length,
                                projScan, null); // No filter here

        // 2. Sort iterator
        // **FIX: Pass K (number of results) to the Sort iterator**
        sortIterator = new iterator.Sort(
            attrTypes,         // Schema of tuples from fileScan
            (short)relDesc.attrCnt,
            strSizes,
            fileScan,          // Input iterator
            queryAttrNum,      // Sort field number (QA)
            new TupleOrder(TupleOrder.Ascending), // Sort by ascending distance
            sortFieldLength,   // Length of the sort field (vector)
            bufferPages,       // Buffer pages
            targetVec,         // Target vector for distance calculation
            k                  // Pass K (number of results)
        );

        // --- Process Sorted Results ---
        // **FIX: Update print statement to reflect K**
        System.out.println("Sort query results (Top " + (k > 0 ? k : "All") + " nearest neighbors):");
        System.out.println("---------------------------------------------");
        System.out.println("Distance | Tuple");
        System.out.println("---------------------------------------------");

        Tuple sortedTuple;
        int resultCount = 0;
        Tuple outputTuple = new Tuple(); // Reusable tuple for projection
        // Set header for the output tuple based on the projected schema
        outputTuple.setHdr((short)outFldCnt, outTypes, outStrSizes);


        // The Sort iterator will stop after K results if K > 0
        while ((sortedTuple = sortIterator.get_next()) != null) {
            // Calculate distance for the current tuple (needed for display)
            double distance = -1.0;
            try {
                int[] vectorData = sortedTuple.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                distance = targetVec.distanceTo(vecObj);
            } catch (Exception e) {
                System.err.println("Warning: Could not calculate distance for a sorted tuple. Skipping.");
                continue;
            }

            // **FIX: Remove the distance threshold (D) filtering**
            // The Sort iterator already limits to K results if K > 0.

            // Project the tuple according to the output specification
            Projection.Project(sortedTuple, attrTypes, outputTuple, projlistOutput, outFldCnt);
            // The outputTuple now has the projected fields. Use its schema (outTypes, outStrSizes)
            // **FIX: Use outTypes for tupleToString**
            System.out.printf("%.2f | %s%n", distance, tupleToString(outputTuple, projlistOutput, outTypes));
            resultCount++;

        }

        // Print summary
        System.out.println("---------------------------------------------");
        // **FIX: Update summary message**
        System.out.println("Total records returned: " + resultCount + (k > 0 ? " (limited to K=" + k + ")" : ""));

    } finally {
        // Close the Sort iterator (which should close the underlying FileScan)
        if (sortIterator != null) {
            sortIterator.close();
        }
        // Defensive closing, though Sort should handle it.
        // else if (fileScan != null) {
        //     fileScan.close();
        // }
    }
}

/**
 * Read a vector from a file.
 * 
 * @param filename The file containing the vector data
 * @return An array of integers representing the vector
 * @throws Exception if there's an error reading the file
 */
private static int[] readVectorFromFile(String filename) throws Exception {
    BufferedReader reader = null;
    try {
        reader = new BufferedReader(new FileReader(filename));
        String line = reader.readLine();
        if (line == null) {
            throw new Exception("Vector file is empty: " + filename);
        }

        // Handle potential extra spaces and split
        String[] values = line.trim().split("\\s+");
        if (values.length != 100) {
             // Check if the first element is empty string due to leading space
             if (values.length == 101 && values[0].isEmpty()) {
                 values = Arrays.copyOfRange(values, 1, 101);
             } else {
                throw new Exception("Vector file must contain exactly 100 integer values, found " + values.length + " in: " + filename);
             }
        }

        int[] vector = new int[100];
        for (int i = 0; i < 100; i++) {
            try {
                vector[i] = Integer.parseInt(values[i]);
            } catch (NumberFormatException e) {
                throw new Exception("Invalid integer value '" + values[i] + "' at position " + i + " in vector file: " + filename);
            }
        }

        return vector;
    } finally {
        if (reader != null) {
             try { reader.close(); } catch (IOException e) {}
        }
    }
}

/**
 * Create a projection list based on output field specification.
 * 
 * @param outputFields Array of field numbers or null for all fields
 * @param attrCount Total number of attributes in the relation
 * @return FldSpec array representing the projection list
 */
private static FldSpec[] createProjectionList(String[] outputFields, int attrCount) {
    FldSpec[] projlist;

    if (outputFields == null || outputFields.length == 0) {
        // Use all fields
        projlist = new FldSpec[attrCount];
        for (int i = 0; i < attrCount; i++) {
             projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }
    } else {
        // Use specified fields
        projlist = new FldSpec[outputFields.length];
        for (int i = 0; i < outputFields.length; i++) {
             try {
                 int fieldNum = Integer.parseInt(outputFields[i].trim());
                 if (fieldNum < 1 || fieldNum > attrCount) {
                     throw new IllegalArgumentException("Output field number " + fieldNum + " out of range (1-" + attrCount + ")");
                 }
                 projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), fieldNum);
             } catch (NumberFormatException e) {
                 throw new IllegalArgumentException("Invalid output field number: " + outputFields[i]);
             }
        }
    }

    return projlist;
}


/**
 * Convert a tuple to a readable string representation.
 * 
 * @param tuple The tuple to convert
 * @param projlist Projection list specifying fields to include
 * @param attrTypes Array of attribute types
 * @return String representation of the tuple
 */
private static String tupleToString(Tuple tuple, FldSpec[] projlist, AttrType[] attrTypes) throws Exception {
    StringBuilder sb = new StringBuilder("[");

    for (int i = 0; i < projlist.length; i++) {
        int fieldNum = projlist[i].offset; // 1-based field number

        if (i > 0) {
            sb.append(", ");
        }

        // Get type from the original schema using fieldNum - 1
        AttrType fieldType = attrTypes[fieldNum-1];

        try {
            switch (fieldType.attrType) {
                case AttrType.attrInteger:
                    sb.append(tuple.getIntFld(fieldNum));
                    break;
                case AttrType.attrReal:
                    sb.append(tuple.getFloFld(fieldNum));
                    break;
                case AttrType.attrString:
                    sb.append(tuple.getStrFld(fieldNum));
                    break;
                case AttrType.attrVector100D:
                    int[] vec = tuple.getVectorFld(fieldNum);
                    // Print only first few elements for brevity
                    sb.append("Vec{");
                    for(int j=0; j<Math.min(vec.length, 5); j++) {
                        sb.append(vec[j]).append(j < Math.min(vec.length, 5) - 1 ? "," : "");
                    }
                    if (vec.length > 5) sb.append("...");
                    sb.append("}");
                    break;
                default:
                     sb.append("?"); // Unknown type
            }
        } catch (FieldNumberOutOfBoundException e) {
             // This shouldn't happen if projlist is correct
             sb.append("ERR_FLD");
        }
    }

    sb.append("]");
    return sb.toString();
}



/**
 * Execute a Filter query on non-vector data.
 * Format: Filter(QA, T, I, ...)
 * 
 * @param queryExpr The full query expression
 * @param relName The name of the relation to query
 * @param bufferPages The number of buffer pages allocated for this query
 * @throws Exception if there's an error executing the query
 */
private static void executeFilterQuery(String queryExpr, String relName, int bufferPages) throws Exception {
    // NOTE: bufferPages is passed but not directly used by FileScan or IndexScan constructors.
    // It influences the global buffer pool size set during 'open database'.
    String[] params = parseQueryParams(queryExpr);

    // **FIX: Expect K as the 3rd parameter, I as the 4th**
    if (params.length < 4) { // Changed from 3 to 4
        throw new Exception("Filter query requires at least 4 parameters: QA, T, K, I, [output fields]"); // Added K
    }

    // Parse parameters
    int queryAttrNum = Integer.parseInt(params[0].trim());
    String targetValue = params[1].trim();
    // **FIX: Parse K**
    int k = Integer.parseInt(params[2].trim()); // K - Number of results
    String indexOption = params[3].trim(); // I is now the 4th parameter

    if (k < 0) {
        System.out.println("Warning: K is negative ("+ k +"). Interpreting as return all matching results (k=0).");
        k = 0;
    } else if (k == 0) {
        System.out.println("Note: K=0 specified. Returning all matching results.");
    }

    // Parse output fields
    String[] outputFields = null;
    // **FIX: Check params.length > 4 for output fields**
    if (params.length > 4) {
        String outputSpec = params[4].trim(); // Output spec is now the 5th parameter
        if (outputSpec.equals("*")) {
            // All fields - will be handled later
            outputFields = null;
        } else {
            // Parse list of field numbers
            outputFields = outputSpec.split("\\s+");
        }
    }

    // Get relation information
    RelDesc relDesc = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName, relDesc);

    // Get attribute information
    AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrDescs[i] = new AttrDesc();
    }
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName, relDesc.attrCnt, attrDescs);

    // Verify query attribute is valid
    if (queryAttrNum < 1 || queryAttrNum > relDesc.attrCnt) {
        throw new Exception("Invalid query attribute number: " + queryAttrNum);
    }

    // Ensure field is not a vector (requirement for Filter query)
    if (attrDescs[queryAttrNum-1].attrType.attrType == AttrType.attrVector100D) {
        throw new Exception("Filter query requires a non-vector attribute");
    }

    // Setup query attributes
    AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
    int strCount = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrTypes[i] = attrDescs[i].attrType;
        if (attrTypes[i].attrType == AttrType.attrString) {
            strCount++;
        }
    }

    // Create string sizes array
    short[] strSizes = new short[strCount];
    int strIndex = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        if (attrTypes[i].attrType == AttrType.attrString) {
            strSizes[strIndex++] = (short) attrDescs[i].attrLen;
        }
    }

    // Setup projection
    FldSpec[] projlist = createProjectionList(outputFields, relDesc.attrCnt);

    // Create condition for filtering
    CondExpr[] expr = new CondExpr[2]; // We need 2: one for the condition, one for null termination
    expr[0] = new CondExpr();
    expr[0].op = new AttrOperator(AttrOperator.aopEQ); // Equality operator
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryAttrNum);

    // Set operand2 based on the attribute type
    AttrType queryAttrActualType = attrTypes[queryAttrNum-1];
    switch (queryAttrActualType.attrType) {
        case AttrType.attrInteger:
            expr[0].type2 = new AttrType(AttrType.attrInteger);
            expr[0].operand2.integer = Integer.parseInt(targetValue);
            break;
        case AttrType.attrReal:
            expr[0].type2 = new AttrType(AttrType.attrReal);
            expr[0].operand2.real = Float.parseFloat(targetValue);
            break;
        case AttrType.attrString:
            expr[0].type2 = new AttrType(AttrType.attrString);
            expr[0].operand2.string = targetValue;
            break;
        default:
            throw new Exception("Unsupported attribute type for filter");
    }

    expr[1] = null; // Mark the end of the conditions

    // Determine if we should use index
    boolean useIndex = indexOption.equalsIgnoreCase("H");

    iterator.Iterator scan = null;
    try {
        // Create appropriate scan based on index option
        if (useIndex) {
            // Check if BTree index exists for this attribute
            boolean indexFound = false;
            int indexCount = relDesc.indexCnt;

            if (indexCount > 0) {
                IndexDesc[] indexDescs = new IndexDesc[indexCount];
                for (int i = 0; i < indexCount; i++) {
                    indexDescs[i] = new IndexDesc();
                }

                try {
                    ExtendedSystemDefs.MINIBASE_INDCAT.getRelInfo(relName, indexCount, indexDescs);

                    // Find matching index for our attribute
                    for (int i = 0; i < indexCount; i++) {
                        // Find the attribute position for this index
                        int indexAttrPos = -1;
                        for (int j = 0; j < relDesc.attrCnt; j++) {
                            if (attrDescs[j].attrName.equals(indexDescs[i].attrName)) {
                                indexAttrPos = j + 1; // Convert to 1-based
                                break;
                            }
                        }

                        // If this index matches our query attribute and is a BTree
                        if (indexAttrPos == queryAttrNum &&
                            indexDescs[i].accessType.indexType == IndexType.B_Index) {

                            // *** START CHANGE: Adjust CondExpr for Real->Integer BTree ***
                            if (queryAttrActualType.attrType == AttrType.attrReal) {
                                System.out.println("DEBUG: Adjusting CondExpr for Real->Integer BTree index scan.");
                                // Modify the expression to use Integer type and casted value
                                expr[0].type2 = new AttrType(AttrType.attrInteger);
                                // Re-parse and cast the target value
                                expr[0].operand2.integer = (int)Float.parseFloat(targetValue);
                                // Clear the float value just in case
                                expr[0].operand2.real = 0.0f;
                            }
                            // *** END CHANGE ***

                            // Get index name
                            String indexName = ExtendedSystemDefs.MINIBASE_INDCAT.buildIndexName(
                                relName, indexDescs[i].attrName, indexDescs[i].accessType);

                            // Create index scan (now using the potentially modified expr)
                            scan = new IndexScan(
                                new IndexType(IndexType.B_Index),
                                relName,
                                indexName,
                                attrTypes,
                                strSizes,
                                relDesc.attrCnt,
                                projlist.length,
                                projlist,
                                expr, // Pass the potentially modified expr
                                queryAttrNum,  // field number of the indexed field
                                false         // false = return full tuple, not just index key
                            );

                            indexFound = true;
                            System.out.println("Using BTree index for filter query");
                            break; // Found suitable index, stop searching
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Error accessing index information: " + e.getMessage());
                }
            }

            if (!indexFound) {
                System.out.println("No suitable index found, using sequential scan");
                // ** Ensure expr is reset if it was modified for index but index wasn't used **
                // (Re-create based on original type if needed, though FileScan might handle mismatch gracefully)
                // For simplicity, let's assume FileScan handles the original expr correctly.
                scan = new FileScan(relName, attrTypes, strSizes,
                    (short) relDesc.attrCnt, (short) projlist.length,
                    projlist, expr); // Pass the original or modified expr
            }
        } else {
            // Use FileScan (no index)
            System.out.println("Using sequential scan for filter query");
            scan = new FileScan(relName, attrTypes, strSizes,
                (short) relDesc.attrCnt, (short) projlist.length,
                projlist, expr); // Pass the original expr
        }

        // Print header
        // **FIX: Update print statement to reflect K**
        System.out.println("Filter query results" + (k > 0 ? " (Top " + k + ")" : "") + ":");
        System.out.println("--------------------");

        // Retrieve and print results
        int resultCount = 0;
        Tuple tuple = null;

        // **FIX: Add check for K in the loop**
        while ((k == 0 || resultCount < k) && (tuple = scan.get_next()) != null) {
            System.out.println(tupleToString(tuple, projlist, attrTypes));
            resultCount++; // Increment count after processing a tuple
        }

        // Print summary
        System.out.println("--------------------");
        // **FIX: Update summary message**
        System.out.println("Total records returned: " + resultCount + (k > 0 ? " (limited to K=" + k + ")" : ""));

    } finally {
        // Close the scan
        if (scan != null) {
            scan.close();
        }
    }
}

/**
 * Execute a Range query on vector data.
 * Format: Range(QA, T, D, I, ...)
 * 
 * @param queryExpr The full query expression
 * @param relName The name of the relation to query
 * @param bufferPages The number of buffer pages allocated for this query
 * @throws Exception if there's an error executing the query
 */
private static void executeRangeQuery(String queryExpr, String relName, int bufferPages) throws Exception {
    // NOTE: bufferPages is passed but not directly used by FileScan or RSIndexScan constructors.
    String[] params = parseQueryParams(queryExpr);
    
    if (params.length < 4) {
        throw new Exception("Range query requires at least 4 parameters: QA, T, D, I, [output fields]");
    }
    
    // Parse parameters
    int queryAttrNum = Integer.parseInt(params[0].trim());
    String targetVectorFile = params[1].trim();
    int rangeDistance = Integer.parseInt(params[2].trim());
    String indexOption = params[3].trim();
    
    if (rangeDistance < 0) {
        throw new Exception("Range distance D must be non-negative");
    }
    
    // Parse output fields
    String[] outputFields = null;
    if (params.length > 4) {
        String outputSpec = params[4].trim();
        if (outputSpec.equals("*")) {
            // All fields - will be handled later
            outputFields = null;
        } else {
            // Parse list of field numbers
            outputFields = outputSpec.split("\\s+");
        }
    }
    
    // Read target vector from file
    int[] targetVector = readVectorFromFile(targetVectorFile);
    Vector100Dtype targetVec = new Vector100Dtype(targetVector);
    
    // Get relation information
    RelDesc relDesc = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName, relDesc);
    
    // Get attribute information
    AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrDescs[i] = new AttrDesc();
    }
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName, relDesc.attrCnt, attrDescs);
    
    // Verify query attribute is a vector
    if (queryAttrNum < 1 || queryAttrNum > relDesc.attrCnt) {
        throw new Exception("Invalid query attribute number: " + queryAttrNum);
    }
    
    if (attrDescs[queryAttrNum-1].attrType.attrType != AttrType.attrVector100D) {
        throw new Exception("Range query attribute must be a vector");
    }
    
    // Setup query attributes
    AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
    int strCount = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrTypes[i] = attrDescs[i].attrType;
        if (attrTypes[i].attrType == AttrType.attrString) {
            strCount++;
        }
    }
    
    // Create string sizes array
    short[] strSizes = new short[strCount];
    int strIndex = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        if (attrTypes[i].attrType == AttrType.attrString) {
            strSizes[strIndex++] = (short) attrDescs[i].attrLen;
        }
    }
    
    // Setup projection
    FldSpec[] projlist = createProjectionList(outputFields, relDesc.attrCnt);
    
    // Determine if we should use LSH index
    boolean useIndex = indexOption.equalsIgnoreCase("H");
    
    iterator.Iterator scan = null;
    int resultCount = 0;
    String indexNameToUse = null; // Store the index name if found
    
    try {
        if (useIndex) {
            indexNameToUse = findLSHIndex(relName, queryAttrNum, relDesc, attrDescs);
            if (indexNameToUse != null) {
                System.out.println("Using LSH index for range query: " + indexNameToUse);
                // Create RSIndexScan
                scan = new RSIndexScan(
                    new IndexType(IndexType.LSHFIndex),
                    relName,
                    indexNameToUse,
                    attrTypes,
                    strSizes,
                    relDesc.attrCnt,
                    projlist.length,
                    projlist,
                    null, // No selection conditions for RSIndexScan itself
                    queryAttrNum,
                    targetVec,
                    rangeDistance
                );
            } else {
                System.out.println("No suitable LSH index found, using sequential scan");
                useIndex = false; // Fallback to sequential scan
            }
        }
        
        // If we couldn't use an index (or chose not to), use sequential scan
        if (scan == null) {
            System.out.println("Using sequential scan for range query");
            // Regular sequential scan with manual filtering needed later
            scan = new FileScan(relName, attrTypes, strSizes,
                (short) relDesc.attrCnt, (short) projlist.length,
                projlist, null);
        }
        
        // Print header
        System.out.println("Range query results (max distance: " + rangeDistance + "):");
        System.out.println("---------------------------------------------");
        System.out.println("Distance | Tuple");
        System.out.println("---------------------------------------------");
        
        // Process results
        Tuple tuple = null;
        while ((tuple = scan.get_next()) != null) {
            // Calculate distance regardless of scan type for printing
            double distance = -1.0; // Default invalid distance
             try {
                int[] vectorData = tuple.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                distance = targetVec.distanceTo(vecObj);
             } catch (Exception e) {
                 System.err.println("Warning: Could not calculate distance for a tuple. Skipping.");
                 continue;
             }


            // If using FileScan, we need to filter manually
            if (!useIndex) {
                if (distance > rangeDistance) {
                    continue; // Skip if outside the range
                }
            }
            // If using RSIndexScan, it's already filtered, just print

            System.out.printf("%.2f | %s%n", distance, tupleToString(tuple, projlist, attrTypes));
            resultCount++;
        }
        
        // Print summary
        System.out.println("---------------------------------------------");
        System.out.println("Total records found: " + resultCount);
        
    } finally {
        // Close the scan
        if (scan != null) {
            scan.close();
        }
    }
}


/**
 * Execute a Nearest Neighbor query on vector data.
 * Format: NN(QA, T, K, I, ...)
 * 
 * @param queryExpr The full query expression
 * @param relName The name of the relation to query
 * @param bufferPages The number of buffer pages allocated for this query
 * @throws Exception if there's an error executing the query
 */
private static void executeNNQuery(String queryExpr, String relName, int bufferPages) throws Exception {
    // NOTE: bufferPages is passed but not directly used by FileScan or NNIndexScan constructors.
    String[] params = parseQueryParams(queryExpr);
    
    if (params.length < 4) {
        throw new Exception("NN query requires at least 4 parameters: QA, T, K, I, [output fields]");
    }
    
    // Parse parameters
    int queryAttrNum = Integer.parseInt(params[0].trim());
    String targetVectorFile = params[1].trim();
    int k = Integer.parseInt(params[2].trim());
    String indexOption = params[3].trim();
    
    if (k < 0) {
        throw new Exception("NN parameter K must be non-negative");
    }
    
    // Parse output fields
    String[] outputFields = null;
    if (params.length > 4) {
        String outputSpec = params[4].trim();
        if (outputSpec.equals("*")) {
            // All fields - will be handled later
            outputFields = null;
        } else {
            // Parse list of field numbers
            outputFields = outputSpec.split("\\s+");
        }
    }
    
    // Read target vector from file
    int[] targetVector = readVectorFromFile(targetVectorFile);
    Vector100Dtype targetVec = new Vector100Dtype(targetVector);
    
    // Get relation information
    RelDesc relDesc = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName, relDesc);
    
    // Get attribute information
    AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrDescs[i] = new AttrDesc();
    }
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName, relDesc.attrCnt, attrDescs);
    
    // Verify query attribute is a vector
    if (queryAttrNum < 1 || queryAttrNum > relDesc.attrCnt) {
        throw new Exception("Invalid query attribute number: " + queryAttrNum);
    }
    
    if (attrDescs[queryAttrNum-1].attrType.attrType != AttrType.attrVector100D) {
        throw new Exception("NN query attribute must be a vector");
    }
    
    // Setup query attributes
    AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
    int strCount = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrTypes[i] = attrDescs[i].attrType;
        if (attrTypes[i].attrType == AttrType.attrString) {
            strCount++;
        }
    }
    
    // Create string sizes array
    short[] strSizes = new short[strCount];
    int strIndex = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        if (attrTypes[i].attrType == AttrType.attrString) {
            strSizes[strIndex++] = (short) attrDescs[i].attrLen;
        }
    }
    
    // Setup projection
    FldSpec[] projlist = createProjectionList(outputFields, relDesc.attrCnt);
    
    // Determine if we should use LSH index
    boolean useIndex = indexOption.equalsIgnoreCase("H");
    
    iterator.Iterator scan = null;
    String indexNameToUse = null;
    
    try {
        if (useIndex) {
            indexNameToUse = findLSHIndex(relName, queryAttrNum, relDesc, attrDescs);
            if (indexNameToUse != null) {
                System.out.println("Using LSH index for NN query: " + indexNameToUse);
                // Create NNIndexScan
                scan = new NNIndexScan(
                    new IndexType(IndexType.LSHFIndex),
                    relName,
                    indexNameToUse,
                    attrTypes,
                    strSizes,
                    relDesc.attrCnt,
                    projlist.length,
                    projlist,
                    null,      // No condition expressions needed
                    queryAttrNum,
                    targetVec,
                    k          // Number of nearest neighbors to return
                );
            } else {
                System.out.println("No suitable LSH index found, using sequential scan");
                useIndex = false; // Fallback to sequential scan
            }
        }
        
        // If no index available or chosen, use sequential scan with manual sorting
        if (scan == null) {
            System.out.println("Performing sequential scan for NN query");
            
            // Use FileScan for full relation scan
            FileScan fileScan = new FileScan(relName, attrTypes, strSizes,
                (short) relDesc.attrCnt, (short) projlist.length,
                projlist, null);
            
            // Read all tuples, calculate distances and store them
            List<Tuple> allTuples = new ArrayList<>();
            List<Double> distances = new ArrayList<>();
            Tuple tuple;
            while ((tuple = fileScan.get_next()) != null) {
                // Make a copy
                Tuple tupleCopy = new Tuple(tuple.getTupleByteArray(), tuple.getOffset(), tuple.getLength());
                tupleCopy.setHdr((short)attrTypes.length, attrTypes, strSizes);
                
                // Calculate distance
                int[] vectorData = tupleCopy.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                double distance = targetVec.distanceTo(vecObj);
                
                allTuples.add(tupleCopy);
                distances.add(distance);
            }
            fileScan.close(); // Close the filescan
            
            // Sort indices by corresponding distances
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < allTuples.size(); i++) {
                indices.add(i);
            }
            indices.sort(Comparator.comparing(distances::get));
            
            // Print header
            System.out.println("NN query results (k=" + k + "):");
            System.out.println("---------------------------------------------");
            System.out.println("Distance | Tuple");
            System.out.println("---------------------------------------------");
            
            // Output k nearest neighbors or all if k=0
            int resultCount = (k == 0) ? allTuples.size() : Math.min(k, allTuples.size());
            for (int i = 0; i < resultCount; i++) {
                int idx = indices.get(i);
                System.out.printf("%.2f | %s%n", distances.get(idx),
                    tupleToString(allTuples.get(idx), projlist, attrTypes));
            }
            
            // Print summary
            System.out.println("---------------------------------------------");
            System.out.println("Total records found: " + resultCount);
            
        } else {
            // Process indexed results (already limited to k nearest neighbors by NNIndexScan)
            System.out.println("NN query results (k=" + k + "):");
            System.out.println("---------------------------------------------");
            System.out.println("Distance | Tuple");
            System.out.println("---------------------------------------------");
            
            int resultCount = 0;
            Tuple tuple;
            while ((tuple = scan.get_next()) != null) {
                // Calculate distance for display
                double distance = -1.0;
                try {
                    int[] vectorData = tuple.getVectorFld(queryAttrNum);
                    Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                    distance = targetVec.distanceTo(vecObj);
                } catch (Exception e) {
                     System.err.println("Warning: Could not calculate distance for a tuple. Skipping.");
                     continue;
                }
                
                System.out.printf("%.2f | %s%n", distance,
                    tupleToString(tuple, projlist, attrTypes));
                resultCount++;
            }
            
            // Print summary
            System.out.println("---------------------------------------------");
            System.out.println("Total records found: " + resultCount);
        }
    }
    finally {
        // Close the scan if it was created and not already closed (e.g., NNIndexScan)
        if (scan != null) {
            scan.close();
        }
    }
}


/**
 * Execute a Distance Join query.
 * Format: DJOIN(OuterSpec, QA2, D, I2, [OutputFields])
 * OuterSpec can be QA1 (int) or Range(...) or NN(...)
 * 
 * @param queryExpr The full query expression
 * @param defaultRelName1 Default outer relation name (used if OuterSpec is just QA1)
 * @param relName2 Inner relation name
 * @param bufferPages The number of buffer pages allocated for this query
 * @throws Exception if there's an error executing the query
 */
private static void executeDJoinQuery(String queryExpr, String defaultRelName1, String relName2, int bufferPages) throws Exception {
    String[] params = parseQueryParams(queryExpr);

    if (params.length < 4) {
        throw new Exception("DJOIN query requires at least 4 parameters: OuterSpec, QA2, D, I2, [OutputFields]");
    }

    String outerSpec = params[0].trim();
    int queryAttr2 = Integer.parseInt(params[1].trim()); // QA2
    int distanceThreshold = Integer.parseInt(params[2].trim()); // D
    String indexOptionInner = params[3].trim(); // I2

    if (distanceThreshold < 0) {
        throw new Exception("Distance Join threshold D must be non-negative");
    }

    // Parse output fields specification
    String[] outputFieldsSpec = null;
    if (params.length > 4) {
        String outputSpec = params[4].trim();
        if (!outputSpec.equals("*")) {
            outputFieldsSpec = outputSpec.split("\\s+");
        }
    }

    OuterQueryResult outerResult;
    String outerRelName;
    int queryAttr1; // QA1

    // Determine if the outer spec is a nested query or just an attribute number
    if (outerSpec.startsWith("Range(") || outerSpec.startsWith("NN(")) {
        // --- Outer is a nested query ---
        String[] outerParams = parseQueryParams(outerSpec);
        if (outerParams.length < 2) throw new Exception("Nested outer query needs at least QA and T parameters");

        queryAttr1 = Integer.parseInt(outerParams[0].trim()); // QA1 from nested query
        // The second parameter of Range/NN is the target vector file, NOT the relation name.
        // The relation name for the outer query must be defaultRelName1.
        outerRelName = defaultRelName1;

        // Pass bufferPages to the helper method
        outerResult = getOuterIteratorAndSchema(outerSpec, outerRelName, bufferPages);

    } else {
         // --- Outer is the full relation (defaultRelName1) ---
         try {
             queryAttr1 = Integer.parseInt(outerSpec); // QA1 is just the attribute number
             outerRelName = defaultRelName1;

             // Get schema for the full outer relation
             RelDesc relDesc1 = new RelDesc();
             ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(outerRelName, relDesc1);
             AttrDesc[] attrDescs1 = new AttrDesc[relDesc1.attrCnt];
             for (int i = 0; i < relDesc1.attrCnt; i++) attrDescs1[i] = new AttrDesc();
             ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(outerRelName, relDesc1.attrCnt, attrDescs1);

             AttrType[] attrTypes1 = new AttrType[relDesc1.attrCnt];
             int strCount1 = 0;
             for (int i = 0; i < relDesc1.attrCnt; i++) {
                 attrTypes1[i] = attrDescs1[i].attrType;
                 if (attrTypes1[i].attrType == AttrType.attrString) strCount1++;
             }
             short[] strSizes1 = new short[strCount1];
             int strIdx1 = 0;
             for (int i = 0; i < relDesc1.attrCnt; i++) {
                 if (attrTypes1[i].attrType == AttrType.attrString) strSizes1[strIdx1++] = (short) attrDescs1[i].attrLen;
             }

             // Create a FileScan for the full outer relation
             // Project all fields initially for the outer scan
             FldSpec[] projOuter = new FldSpec[relDesc1.attrCnt];
             for (int i = 0; i < relDesc1.attrCnt; i++) projOuter[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);

             FileScan outerScan = new FileScan(outerRelName, attrTypes1, strSizes1,
                                              (short)relDesc1.attrCnt, (short)relDesc1.attrCnt,
                                              projOuter, null);

             outerResult = new OuterQueryResult(outerScan, attrTypes1, strSizes1, relDesc1.attrCnt);

         } catch (NumberFormatException e) {
             throw new Exception("Invalid OuterSpec for DJOIN: Must be Range(...), NN(...), or a column number (QA1).");
         }
    }

    // --- Get Inner Relation Schema ---
    RelDesc relDesc2 = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName2, relDesc2);
    AttrDesc[] attrDescs2 = new AttrDesc[relDesc2.attrCnt];
    for (int i = 0; i < relDesc2.attrCnt; i++) attrDescs2[i] = new AttrDesc();
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName2, relDesc2.attrCnt, attrDescs2);

    AttrType[] attrTypes2 = new AttrType[relDesc2.attrCnt];
    int strCount2 = 0;
    for (int i = 0; i < relDesc2.attrCnt; i++) {
        attrTypes2[i] = attrDescs2[i].attrType;
        if (attrTypes2[i].attrType == AttrType.attrString) strCount2++;
    }
    short[] strSizes2 = new short[strCount2];
    int strIdx2 = 0;
    for (int i = 0; i < relDesc2.attrCnt; i++) {
        if (attrTypes2[i].attrType == AttrType.attrString) strSizes2[strIdx2++] = (short) attrDescs2[i].attrLen;
    }

    // --- Verify Join Attributes ---
    if (queryAttr1 < 1 || queryAttr1 > outerResult.numAttrs || outerResult.attrTypes[queryAttr1-1].attrType != AttrType.attrVector100D) {
        throw new Exception("Invalid vector attribute number QA1 ("+ queryAttr1 +") for outer relation " + outerRelName);
    }
    if (queryAttr2 < 1 || queryAttr2 > relDesc2.attrCnt || attrTypes2[queryAttr2-1].attrType != AttrType.attrVector100D) {
        throw new Exception("Invalid vector attribute number QA2 ("+ queryAttr2 +") for inner relation " + relName2);
    }

    // --- Setup Projection List for Join Output ---
    FldSpec[] projListJoin;
    int outFldCnt;
    AttrType[] jTypes; // Schema of the joined output tuple
    short[] jStrSizes = null; // String sizes for the joined output

    if (outputFieldsSpec == null) { // Project all fields
        outFldCnt = outerResult.numAttrs + relDesc2.attrCnt;
        projListJoin = new FldSpec[outFldCnt];
        jTypes = new AttrType[outFldCnt];
        List<Short> tempStrSizes = new ArrayList<>();
        int k = 0;
        int outerStrIdx = 0;
        for (int i = 0; i < outerResult.numAttrs; i++) {
            projListJoin[k] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
            jTypes[k] = outerResult.attrTypes[i];
            if (jTypes[k].attrType == AttrType.attrString) {
                tempStrSizes.add(outerResult.strSizes[outerStrIdx++]);
            }
            k++;
        }
        int innerStrIdx = 0;
        for (int i = 0; i < relDesc2.attrCnt; i++) {
            projListJoin[k] = new FldSpec(new RelSpec(RelSpec.innerRel), i + 1);
            jTypes[k] = attrTypes2[i];
             if (jTypes[k].attrType == AttrType.attrString) {
                tempStrSizes.add(strSizes2[innerStrIdx++]);
            }
            k++;
        }
        jStrSizes = tempStrSizes.stream().mapToInt(s -> s).collect(() -> new short[tempStrSizes.size()], (arr, val) -> arr[arr.length - tempStrSizes.indexOf((short)val)] = (short)val, (arr1, arr2) -> {}); // Convert List<Short> to short[] - simplified logic needed here
         // Correct conversion from List<Short> to short[]
         jStrSizes = new short[tempStrSizes.size()];
         for(int i=0; i<tempStrSizes.size(); i++) jStrSizes[i] = tempStrSizes.get(i);


    } else { // Project specified fields (O#/I#)
        outFldCnt = outputFieldsSpec.length;
        projListJoin = new FldSpec[outFldCnt];
        jTypes = new AttrType[outFldCnt];
        List<Short> tempStrSizes = new ArrayList<>();
        int outerStrIdx = 0;
        int innerStrIdx = 0;

        for (int i = 0; i < outFldCnt; i++) {
            String fieldSpec = outputFieldsSpec[i].trim().toUpperCase();
            if (fieldSpec.length() < 2) throw new Exception("Invalid field spec: " + fieldSpec);
            char relType = fieldSpec.charAt(0);
            int fieldNum = Integer.parseInt(fieldSpec.substring(1));

            if (relType == 'O') {
                if (fieldNum < 1 || fieldNum > outerResult.numAttrs) throw new Exception("Invalid outer field num: " + fieldNum);
                projListJoin[i] = new FldSpec(new RelSpec(RelSpec.outer), fieldNum);
                jTypes[i] = outerResult.attrTypes[fieldNum - 1];
                if (jTypes[i].attrType == AttrType.attrString) {
                    // Find the correct string size from the original outer schema
                    int currentOuterStr = 0;
                    for(int outerAttr=0; outerAttr<fieldNum-1; outerAttr++){
                        if(outerResult.attrTypes[outerAttr].attrType == AttrType.attrString) currentOuterStr++;
                    }
                    tempStrSizes.add(outerResult.strSizes[currentOuterStr]);
                }
            } else if (relType == 'I') {
                if (fieldNum < 1 || fieldNum > relDesc2.attrCnt) throw new Exception("Invalid inner field num: " + fieldNum);
                projListJoin[i] = new FldSpec(new RelSpec(RelSpec.innerRel), fieldNum);
                jTypes[i] = attrTypes2[fieldNum - 1];
                 if (jTypes[i].attrType == AttrType.attrString) {
                     // Find the correct string size from the original inner schema
                    int currentInnerStr = 0;
                    for(int innerAttr=0; innerAttr<fieldNum-1; innerAttr++){
                        if(attrTypes2[innerAttr].attrType == AttrType.attrString) currentInnerStr++;
                    }
                    tempStrSizes.add(strSizes2[currentInnerStr]);
                }
            } else {
                throw new Exception("Invalid relation specifier '" + relType + "' in output fields.");
            }
        }
         // Correct conversion from List<Short> to short[]
         jStrSizes = new short[tempStrSizes.size()];
         for(int i=0; i<tempStrSizes.size(); i++) jStrSizes[i] = tempStrSizes.get(i);
    }
    if (outFldCnt == 0) throw new Exception("No output fields specified for DJOIN.");


    // --- Create Join Condition ---
    CondExpr[] joinExpr = new CondExpr[2];
    joinExpr[0] = new CondExpr();
    joinExpr[0].op = new AttrOperator(AttrOperator.aopVECTORDIST); // Use vector distance operator
    joinExpr[0].type1 = new AttrType(AttrType.attrSymbol);
    joinExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryAttr1); // QA1 from outer
    joinExpr[0].type2 = new AttrType(AttrType.attrSymbol);
    joinExpr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), queryAttr2); // QA2 from inner
    joinExpr[0].distance = distanceThreshold; // The distance threshold D
    joinExpr[1] = null;

    // --- Determine Inner Index Usage ---
    boolean useInnerIndex = indexOptionInner.equalsIgnoreCase("H");
    String innerIndexName = null;
    if (useInnerIndex) {
        innerIndexName = findLSHIndex(relName2, queryAttr2, relDesc2, attrDescs2);
        if (innerIndexName != null) {
            System.out.println("Using LSH index for inner relation (" + relName2 + "): " + innerIndexName);
        } else {
            System.out.println("No suitable LSH index found for inner relation (" + relName2 + "), using nested loop join.");
            useInnerIndex = false;
        }
    }

    // --- Create Join Iterator ---
    iterator.Iterator joinIterator = null;
    try {
        if (useInnerIndex && innerIndexName != null) {
             System.out.println("Using Index Nested Loop Join (INLJ)");
            joinIterator = new INLJoins(
                outerResult.attrTypes, outerResult.numAttrs, outerResult.strSizes,
                attrTypes2, relDesc2.attrCnt, strSizes2,
                bufferPages, // Use bufferPages
                outerResult.iterator, // The iterator from the outer query (Range/NN/FileScan)
                relName2,
                new IndexType(IndexType.LSHFIndex),
                innerIndexName,
                joinExpr,
                null, // Right filter expression (none here)
                projListJoin,
                outFldCnt
            );
        } else {
             System.out.println("Using Nested Loop Join (NLJ)");
            joinIterator = new NestedLoopsJoins(
                outerResult.attrTypes, outerResult.numAttrs, outerResult.strSizes,
                attrTypes2, relDesc2.attrCnt, strSizes2,
                bufferPages, // Use bufferPages
                outerResult.iterator, // The iterator from the outer query
                relName2,
                joinExpr,
                null, // Right filter expression
                projListJoin,
                outFldCnt
            );
        }

        // --- Process and Print Results ---
        System.out.println("DJOIN results (max distance: " + distanceThreshold + "):");
        System.out.println("---------------------------------------------");

        Tuple outputTuple = new Tuple(); // Create a tuple object to be reused
        outputTuple.setHdr((short) outFldCnt, jTypes, jStrSizes); // Set header once

        int resultCount = 0;
        try {
            while ((outputTuple = joinIterator.get_next()) != null) {
                // The tuple returned by the join should already be projected.
                // Header is already set if the join operator correctly populates the tuple.
                // We might need to ensure the tuple object passed to get_next allows modification,
                // or create a new one each time if necessary. Assuming get_next returns a valid tuple.
                System.out.println(tupleToString(outputTuple, projListJoin, jTypes)); // Use jTypes here
                resultCount++;
            }
        } catch (Exception e) {
            System.err.println("Error getting next join result: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("---------------------------------------------");
        System.out.println("Total joined records found: " + resultCount);

    } finally {
        // --- Close the Join Iterator ---
        // This will also close the underlying outer iterator (Range/NN/FileScan)
        if (joinIterator != null) {
            joinIterator.close();
        }
    }
}


/**
 * Helper method to create the outer iterator and get its schema for DJOIN.
 *
 * @param outerQueryExpr The full query expression for the outer part (Range or NN)
 * @param baseRelName The name of the base relation being queried
 * @param bufferPages The number of buffer pages allocated for this query
 * @return OuterQueryResult containing the iterator and schema info
 * @throws Exception if there's an error setting up the outer query
 */
private static OuterQueryResult getOuterIteratorAndSchema(String outerQueryExpr, String baseRelName, int bufferPages) throws Exception {
    System.out.println("Executing outer query: " + outerQueryExpr);

    // Get relation information
    RelDesc relDesc = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(baseRelName, relDesc);

    // Get attribute information
    AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrDescs[i] = new AttrDesc();
    }
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(baseRelName, relDesc.attrCnt, attrDescs);

    // Setup query attributes
    AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
    int strCount = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        attrTypes[i] = attrDescs[i].attrType;
        if (attrTypes[i].attrType == AttrType.attrString) {
            strCount++;
        }
    }
    short[] strSizes = new short[strCount];
    int strIndex = 0;
    for (int i = 0; i < relDesc.attrCnt; i++) {
        if (attrTypes[i].attrType == AttrType.attrString) {
            strSizes[strIndex++] = (short) attrDescs[i].attrLen;
        }
    }

    // Setup projection - for the outer query used in a join, we need all fields
    FldSpec[] projlist = new FldSpec[relDesc.attrCnt];
    for (int i = 0; i < relDesc.attrCnt; i++) {
        projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
    }
    int projCount = relDesc.attrCnt;


    iterator.Iterator scan = null;
    String[] params = parseQueryParams(outerQueryExpr);

    if (outerQueryExpr.startsWith("Range(")) {
        if (params.length < 4) throw new Exception("Outer Range query requires at least 4 parameters");
        int queryAttrNum = Integer.parseInt(params[0].trim());
        String targetVectorFile = params[1].trim(); // T1 is the target vector file
        int rangeDistance = Integer.parseInt(params[2].trim());
        String indexOption = params[3].trim();

        if (queryAttrNum < 1 || queryAttrNum > relDesc.attrCnt || attrTypes[queryAttrNum-1].attrType != AttrType.attrVector100D) {
             throw new Exception("Invalid vector attribute number for outer Range query: " + queryAttrNum);
        }
        int[] targetVector = readVectorFromFile(targetVectorFile);
        Vector100Dtype targetVec = new Vector100Dtype(targetVector);

        boolean useIndex = indexOption.equalsIgnoreCase("H");
        boolean indexFound = false;
        String indexName = null;

        if (useIndex) {
            indexName = findLSHIndex(baseRelName, queryAttrNum, relDesc, attrDescs);
            if (indexName != null) {
                System.out.println("Using LSH index for outer Range query: " + indexName);
                scan = new RSIndexScan(new IndexType(IndexType.LSHFIndex), baseRelName, indexName,
                                       attrTypes, strSizes, (short)relDesc.attrCnt, (short)projCount, projlist,
                                       null, queryAttrNum, targetVec, rangeDistance);
                indexFound = true;
            } else {
                 System.out.println("No suitable LSH index found for outer Range query, using sequential scan with filter");
            }
        }
        if (!indexFound) {
             // Need to apply range filter manually using FileScan + Filter logic
             // This requires a CondExpr for distance.
             CondExpr[] expr = new CondExpr[2];
             expr[0] = new CondExpr();
             expr[0].op = new AttrOperator(AttrOperator.aopVECTORDIST);
             expr[0].type1 = new AttrType(AttrType.attrSymbol);
             expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryAttrNum);
             expr[0].type2 = new AttrType(AttrType.attrVector100D); // Literal vector
             expr[0].operand2.vector = targetVec; // Store the target vector directly
             expr[0].distance = rangeDistance; // Use distance for comparison (<= D)
             expr[1] = null;

             // FileScan will apply the filter
             scan = new FileScan(baseRelName, attrTypes, strSizes, (short) relDesc.attrCnt, (short) projCount, projlist, expr);
        }

    } else if (outerQueryExpr.startsWith("NN(")) {
        if (params.length < 4) throw new Exception("Outer NN query requires at least 4 parameters");
        int queryAttrNum = Integer.parseInt(params[0].trim());
        String targetVectorFile = params[1].trim(); // T1 is the target vector file
        int k = Integer.parseInt(params[2].trim());
        String indexOption = params[3].trim();

         if (queryAttrNum < 1 || queryAttrNum > relDesc.attrCnt || attrTypes[queryAttrNum-1].attrType != AttrType.attrVector100D) {
             throw new Exception("Invalid vector attribute number for outer NN query: " + queryAttrNum);
        }
        int[] targetVector = readVectorFromFile(targetVectorFile);
        Vector100Dtype targetVec = new Vector100Dtype(targetVector);

        boolean useIndex = indexOption.equalsIgnoreCase("H");
        boolean indexFound = false;
        String indexName = null;

        if (useIndex) {
            indexName = findLSHIndex(baseRelName, queryAttrNum, relDesc, attrDescs);
            if (indexName != null) {
                System.out.println("Using LSH index for outer NN query: " + indexName);
                scan = new NNIndexScan(new IndexType(IndexType.LSHFIndex), baseRelName, indexName,
                                       attrTypes, strSizes, (short)relDesc.attrCnt, (short)projCount, projlist,
                                       null, queryAttrNum, targetVec, k);
                indexFound = true;
            } else {
                 System.out.println("No suitable LSH index found for outer NN query, using sequential scan");
            }
        }
         if (!indexFound) {
             // NN without index requires sorting. For DJOIN, we need the stream.
             // We'll use FileScan and rely on the join operator to handle iteration.
             // A full NN sort here would be inefficient.
             System.out.println("Using sequential scan for outer NN query (results not pre-sorted/limited by K)");
             scan = new FileScan(baseRelName, attrTypes, strSizes, (short) relDesc.attrCnt, (short) projCount, projlist, null);
         }

    } else {
        throw new Exception("Unsupported outer query type for DJOIN: " + outerQueryExpr);
    }

    // Return the iterator and the full schema of the base relation
    return new OuterQueryResult(scan, attrTypes, strSizes, relDesc.attrCnt);
}

/**
 * Helper to find a suitable LSH index file name.
 */
private static String findLSHIndex(String relName, int queryAttrNum, RelDesc relDesc, AttrDesc[] attrDescs) {
     int indexCount = relDesc.indexCnt;
     if (indexCount > 0) {
         IndexDesc[] indexDescs = new IndexDesc[indexCount];
         for (int i = 0; i < indexCount; i++) indexDescs[i] = new IndexDesc();
         try {
             ExtendedSystemDefs.MINIBASE_INDCAT.getRelInfo(relName, indexCount, indexDescs);
             for (int i = 0; i < indexCount; i++) {
                 int indexAttrPos = -1;
                 for (int j = 0; j < relDesc.attrCnt; j++) {
                     if (attrDescs[j].attrName.equals(indexDescs[i].attrName)) {
                         indexAttrPos = j + 1; break;
                     }
                 }
                 if (indexAttrPos == queryAttrNum && indexDescs[i].accessType.indexType == IndexType.LSHFIndex) {
                     // Find the actual .ser file based on naming convention
                     File dir = new File(".");
                     String prefix = relName + queryAttrNum + "_L";
                     File[] matchingFiles = dir.listFiles((d, name) -> name.startsWith(prefix) && name.endsWith(".ser"));
                     if (matchingFiles != null && matchingFiles.length > 0) {
                         return matchingFiles[0].getName(); // Return first match
                     }
                 }
             }
         } catch (Exception e) {
             System.err.println("Warning: Error accessing index info for " + relName + ": " + e.getMessage());
         }
     }
     return null; // No suitable index found
}

    
}