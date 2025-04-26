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
    
    // Current database state
    private static String currentDBName = null;
    private static boolean dbOpen = false;
    
    // Constants for buffer management
    private static final int DEFAULT_DB_PAGES = 5000;
    private static final int DEFAULT_BUFFER_PAGES = 100;
    
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
        // Check if another database is already open
        if (dbOpen) {
            System.out.println("Error: Another database is already open. Close it first.");
            return;
        }
        
        // Reset page counter to track I/O operations
        PCounter.initialize();
        
        try {
            // Check if the database exists by trying to open it
            try {
                // Try to open the existing database
                new SystemDefs(dbname, 0, DEFAULT_BUFFER_PAGES, "Clock");
                System.out.println("Opening existing database: " + dbname);
            } catch (Exception e) {
                // If the database doesn't exist, create a new one
                System.out.println("Database doesn't exist. Creating new database: " + dbname);
                new SystemDefs(dbname, DEFAULT_DB_PAGES, DEFAULT_BUFFER_PAGES, "Clock");
            }
            
            // Set the current database name and state
            currentDBName = dbname;
            dbOpen = true;
            
            // Print I/O statistics
            System.out.println("Database opened successfully.");
            System.out.println("Page reads: " + PCounter.getRCount());
            System.out.println("Page writes: " + PCounter.getWCount());
            
        } catch (Exception e) {
            System.err.println("Error opening database: " + e.getMessage());
            throw e;
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
        if (!dbOpen) {
            System.out.println("Error: No database is open. Please open a database first.");
            return;
        }
        
        // Reset page counter to track I/O operations
        PCounter.initialize();
        
        BufferedReader reader = null;
        Heapfile heapFile = null;
        
        try {
            // Open the data file
            reader = new BufferedReader(new FileReader(dataFileName));
            
            // Read the number of attributes
            int attrCount = Integer.parseInt(reader.readLine().trim());
            if (attrCount <= 0) {
                throw new Exception("Invalid attribute count in data file");
            }
            
            // Read the attribute types
            String[] typeStrs = reader.readLine().trim().split("\\s+");
            if (typeStrs.length != attrCount) {
                throw new Exception("Mismatch between attribute count and type count");
            }
            
            // Convert type strings to AttrType array
            AttrType[] attrTypes = new AttrType[attrCount];
            for (int i = 0; i < attrCount; i++) {
                int typeInt = Integer.parseInt(typeStrs[i]);
                switch (typeInt) {
                    case 1: // Integer
                        attrTypes[i] = new AttrType(AttrType.attrInteger);
                        break;
                    case 2: // Float/Real
                        attrTypes[i] = new AttrType(AttrType.attrReal);
                        break;
                    case 3: // String
                        attrTypes[i] = new AttrType(AttrType.attrString);
                        break;
                    case 4: // Vector
                        attrTypes[i] = new AttrType(AttrType.attrVector100D);
                        break;
                    default:
                        throw new Exception("Invalid attribute type: " + typeInt);
                }
            }
            
            // Count string attributes for size array
            int strCount = 0;
            for (AttrType type : attrTypes) {
                if (type.attrType == AttrType.attrString) {
                    strCount++;
                }
            }
            
            // Create array for string sizes
            short[] strSizes = new short[strCount];
            // We'll use a default size for strings - real implementation would determine from data
            for (int i = 0; i < strCount; i++) {
                strSizes[i] = 32; // default string length
            }
            
            // Create a heap file for the relation
            heapFile = new Heapfile(relationName);
            
            // Create attribute information array for catalog
            attrInfo[] attrInfo = new attrInfo[attrCount];
            for (int i = 0; i < attrCount; i++) {
                attrInfo[i] = new attrInfo();
                attrInfo[i].attrName = "attr" + i;
                attrInfo[i].attrType = attrTypes[i]; 
                
                if (attrTypes[i].attrType == AttrType.attrString) {
                    attrInfo[i].attrLen = 32; // Default string length
                } else if (attrTypes[i].attrType == AttrType.attrInteger) {
                    attrInfo[i].attrLen = 4;
                } else if (attrTypes[i].attrType == AttrType.attrReal) {
                    attrInfo[i].attrLen = 4;
                } else if (attrTypes[i].attrType == AttrType.attrVector100D) {
                    attrInfo[i].attrLen = 400; // 100 integers * 4 bytes
                }
            }
            
            // Add relation to catalog
            try {
                // Use the public createRel method from RelCatalog via ExtendedSystemDefs
                ExtendedSystemDefs.MINIBASE_RELCAT.createRel(relationName, attrCount, attrInfo);
            } catch (Exception e) {
                throw new Exception("Failed to create relation in catalog: " + e.getMessage());
            }
            
            // Read tuples from the data file and insert them
            String line;
            int tupleCount = 0;
            
            while (true) {
                // Create a new tuple for insertion
                Tuple tuple = new Tuple();
                // Setup a tuple template
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
                heapFile.insertRecord(tuple.getTupleByteArray());
                tupleCount++;
            }
            
            // Report success and statistics
            System.out.println("Successfully created table '" + relationName + "' with " + tupleCount + " tuples.");
            System.out.println("Page reads: " + PCounter.getRCount());
            System.out.println("Page writes: " + PCounter.getWCount());
            
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
            
            // Clean up resources if an error occurred
            if (heapFile != null) {
                try {
                    heapFile.deleteFile();
                } catch (Exception ex) {
                    System.err.println("Error deleting heap file during cleanup: " + ex.getMessage());
                }
            }
            
            throw e;
        } finally {
            // Close resources
            if (reader != null) {
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
        
        try {
            // Create a RelDesc object to receive relation info (output parameter pattern)
            RelDesc relDesc = new RelDesc();
            
            // Get relation information using output parameter pattern
            ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relationName, relDesc);
            
            // Check if column ID is valid (assuming 1-based indexing for user input)
            if (columnId < 1 || columnId > relDesc.attrCnt) {
                throw new Exception("Column ID " + columnId + " is out of range (1-" + relDesc.attrCnt + ")");
            }
            
            // Create array for attribute descriptors
            AttrDesc[] attrDescs = new AttrDesc[relDesc.attrCnt];
            
            // Initialize each AttrDesc in the array
            for (int i = 0; i < relDesc.attrCnt; i++) {
                attrDescs[i] = new AttrDesc();
            }
            
            // Get all attribute information
            ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relationName, relDesc.attrCnt, attrDescs);
            
            // Get the specific attribute (converting to 0-based indexing)
            AttrDesc attrDesc = attrDescs[columnId-1];
            
            // Access fields through correct visibility
            AttrType attrType = attrDesc.attrType;
            String attrName = attrDesc.attrName; 
            
            heapFile = new Heapfile(relationName);
            
            // Rest of the code remains the same...
            if (attrType.attrType == AttrType.attrVector100D) {
                // Create an LSH index for vector data
                System.out.println("Creating LSH index with " + lValue + " layers and " + 
                                  hValue + " hash functions per layer...");
                
                // Initialize LSH index
                LSHFIndex lshIndex = new LSHFIndex(hValue, lValue);
                
                // Prepare schema info to read tuples correctly
                AttrType[] attrTypes = new AttrType[relDesc.attrCnt];
                int strCount = 0;
                for (int i = 0; i < relDesc.attrCnt; i++) {
                    attrTypes[i] = attrDescs[i].attrType;
                    if (attrTypes[i].attrType == AttrType.attrString) {
                        strCount++;
                    }
                }
                
                // Create strSizes array for string attributes
                short[] strSizes = new short[strCount];
                int strIndex = 0;
                for (int i = 0; i < relDesc.attrCnt; i++) {
                    if (attrTypes[i].attrType == AttrType.attrString) {
                        strSizes[strIndex++] = (short) attrDescs[i].attrLen;
                    }
                }
                
                // Scan all records and add to LSH index
                scan = heapFile.openScan();
                RID rid = new RID();
                Tuple tuple = null;
                int count = 0;
                
                while ((tuple = scan.getNext(rid)) != null) {
                    // Deep copy the RID since it's reused by scan.getNext()
                    RID ridCopy = new RID(rid.pageNo, rid.slotNo);
                    
                    // Set header to properly interpret tuple data
                    tuple.setHdr((short)attrTypes.length, attrTypes, strSizes);
                    
                    try {
                        // Get vector data (columnId is 1-based for user, but getVectorFld wants 1-based too)
                        int[] vectorData = tuple.getVectorFld(columnId);
                        Vector100Dtype vectorObj = new Vector100Dtype(vectorData);
                        Vector100DKey key = new Vector100DKey(vectorObj);
                        
                        // Insert into LSH index
                        lshIndex.insert(key, ridCopy);
                        count++;
                    } catch (Exception e) {
                        System.err.println("Warning: Could not index tuple: " + e.getMessage());
                    }
                }
                
                // Close the scan before saving index
                scan.closescan();
                scan = null;
                
                // Build index file name: RELNAME + columnNumber + "_L" + L + "_h" + h
                String indexFileName = relationName + columnId + "_L" + lValue + "_h" + hValue + ".ser";
                
                // Save the LSH index
                lshIndex.saveIndex(indexFileName);
                
                // Register the index in the catalog
                // Create a new IndexDesc for this index
                IndexDesc indexDesc = new IndexDesc();
                
                // These fields are package-private but should be accessible from catalog methods
                indexDesc.relName = relationName;
                indexDesc.attrName = attrName;
                indexDesc.accessType = new IndexType(IndexType.LSHFIndex);
                indexDesc.order = new TupleOrder(TupleOrder.Random); 
                indexDesc.clustered = 0;
                
                // Add index info to catalog
                ExtendedSystemDefs.MINIBASE_INDCAT.addInfo(indexDesc);
                
                System.out.println("Created LSH index on " + relationName + "." + attrName + 
                                  " with " + count + " entries.");
                System.out.println("Index saved to file: " + indexFileName);
                
            } else {
                // For BTree index, use the catalog's addIndex method
                String indexName = relationName + columnId + ".btree";
                System.out.println("Creating B-Tree index " + indexName);
                
                // Note: attrName might be needed instead of columnId for the catalog
                ExtendedSystemDefs.MINIBASE_INDCAT.addIndex(relationName, attrName, 
                                    new IndexType(IndexType.B_Index), 0);
                
                System.out.println("Created B-Tree index on " + relationName + "." + attrName);
            }
            
            // Report I/O statistics
            System.out.println("Index creation complete.");
            System.out.println("Disk pages read: " + PCounter.getRCount());
            System.out.println("Disk pages written: " + PCounter.getWCount());
            
        } catch (Exception e) {
            // Close resources if still open
            if (scan != null) {
                try { scan.closescan(); } catch (Exception ex) { /* Ignore cleanup errors */ }
            }
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
                                        key = new StringKey(Float.toString(tuple.getFloFld(indexAttrPos)));
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
                                // Format: relationName + columnId + "_L" + lValue + "_h" + hValue + ".ser"
                                File dir = new File(".");
                                String prefix = relationName + indexAttrPos + "_L";
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
                                        key = new StringKey(Float.toString((Float)keyValue));
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
                                String prefix = relationName + indexAttrPos + "_L";
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
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("Warning: Error updating LSH index: " + e.getMessage());
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
            executeSortQuery(queryLine, relName1);
        } else if (queryLine.startsWith("Filter(")) {
            // Handle Filter query
            executeFilterQuery(queryLine, relName1);
        } else if (queryLine.startsWith("Range(")) {
            // Handle Range query
            executeRangeQuery(queryLine, relName1);
        } else if (queryLine.startsWith("NN(")) {
            // Handle Nearest Neighbor query
            executeNNQuery(queryLine, relName1);
        } else if (queryLine.startsWith("DJOIN(")) {
            // Handle Distance Join query
            executeDJoinQuery(queryLine, relName1, relName2);
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
 * @throws Exception if there's an error executing the query
 */
private static void executeSortQuery(String queryExpr, String relName) throws Exception {
    String[] params = parseQueryParams(queryExpr);
    
    if (params.length < 3) {
        throw new Exception("Sort query requires at least 3 parameters: QA, T, D, [output fields]");
    }
    
    // Parse parameters
    int queryAttrNum = Integer.parseInt(params[0].trim());
    String targetVectorFile = params[1].trim();
    int unusedParam = Integer.parseInt(params[2].trim()); // D parameter (unused)
    
    // Parse output fields
    String[] outputFields = null;
    if (params.length > 3) {
        String outputSpec = params[3].trim();
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
        throw new Exception("Query attribute must be a vector");
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
    
    // Create FileScan to read the relation
    FileScan scan = new FileScan(relName, attrTypes, strSizes,
                              (short) relDesc.attrCnt, (short) projlist.length,
                              projlist, null);
    
    // Create Vector100D for target
    Vector100Dtype targetVec = new Vector100Dtype(targetVector);
    
    // Create Sort operator
    // Note: Since we're sorting by distance from target vector, we need to:
    // 1. Read all tuples
    // 2. Calculate distance from target
    // 3. Sort by distance
    
    List<Tuple> allTuples = new ArrayList<>();
    List<Double> distances = new ArrayList<>();
    
    Tuple tuple = null;
    while ((tuple = scan.get_next()) != null) {
        // Make a copy of the tuple
        Tuple tupleCopy = new Tuple(tuple);
        
        // Extract vector and calculate distance
        int[] vectorData = tupleCopy.getVectorFld(queryAttrNum);
        Vector100Dtype vecObj = new Vector100Dtype(vectorData);
        double distance = targetVec.distanceTo(vecObj);
        
        // Store tuple and its distance
        allTuples.add(tupleCopy);
        distances.add(distance);
    }
    
    // Sort tuples by distance
    List<Integer> indices = new ArrayList<>();
    for (int i = 0; i < allTuples.size(); i++) {
        indices.add(i);
    }
    
    // Sort indices by corresponding distances
    indices.sort(Comparator.comparing(distances::get));
    
    // Print header
    System.out.println("Sort query results:");
    System.out.println("-------------------");
    System.out.println("Distance | Tuple");
    System.out.println("-------------------");
    
    // Output sorted tuples
    for (int idx : indices) {
        System.out.printf("%.2f | %s%n", distances.get(idx), tupleToString(allTuples.get(idx), projlist, attrTypes));
    }
    
    // Close resources
    scan.close();
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
            throw new Exception("Vector file is empty");
        }
        
        String[] values = line.trim().split("\\s+");
        if (values.length != 100) {
            throw new Exception("Vector must have exactly 100 values");
        }
        
        int[] vector = new int[100];
        for (int i = 0; i < 100; i++) {
            int val = Integer.parseInt(values[i]);
            if (val < -10000 || val > 10000) {
                throw new Exception("Vector values must be in range -10000 to 10000");
            }
            vector[i] = val;
        }
        
        return vector;
    } finally {
        if (reader != null) {
            try { reader.close(); } catch (Exception e) { /* Ignore */ }
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
            projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }
    } else {
        // Use specified fields
        projlist = new FldSpec[outputFields.length];
        for (int i = 0; i < outputFields.length; i++) {
            int fieldNum = Integer.parseInt(outputFields[i]);
            projlist[i] = new FldSpec(new RelSpec(RelSpec.outer), fieldNum);
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
        int fieldNum = projlist[i].offset;
        
        if (i > 0) {
            sb.append(", ");
        }
        
        switch (attrTypes[fieldNum-1].attrType) {
            case AttrType.attrInteger:
                sb.append(tuple.getIntFld(fieldNum));
                break;
            case AttrType.attrReal:
                sb.append(tuple.getFloFld(fieldNum));
                break;
            case AttrType.attrString:
                sb.append('"').append(tuple.getStrFld(fieldNum)).append('"');
                break;
            case AttrType.attrVector100D:
                sb.append("Vector[...])"); // Abbreviated for display
                break;
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
 * @throws Exception if there's an error executing the query
 */
private static void executeFilterQuery(String queryExpr, String relName) throws Exception {
    String[] params = parseQueryParams(queryExpr);
    
    if (params.length < 3) {
        throw new Exception("Filter query requires at least 3 parameters: QA, T, I, [output fields]");
    }
    
    // Parse parameters
    int queryAttrNum = Integer.parseInt(params[0].trim());
    String targetValue = params[1].trim();
    String indexOption = params[2].trim();
    
    // Parse output fields
    String[] outputFields = null;
    if (params.length > 3) {
        String outputSpec = params[3].trim();
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
    switch (attrTypes[queryAttrNum-1].attrType) {
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
                            
                            // Get index name
                            String indexName = ExtendedSystemDefs.MINIBASE_INDCAT.buildIndexName(
                                relName, indexDescs[i].attrName, indexDescs[i].accessType);
                            
                            // Create index scan
                            scan = new IndexScan(
                                new IndexType(IndexType.B_Index),
                                relName, 
                                indexName,
                                attrTypes, 
                                strSizes,
                                relDesc.attrCnt, 
                                projlist.length,
                                projlist, 
                                expr,
                                queryAttrNum,  // field number of the indexed field
                                false         // false = return full tuple, not just index key
                            );
                            
                            indexFound = true;
                            System.out.println("Using BTree index for filter query");
                            break;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Error accessing index information: " + e.getMessage());
                }
            }
            
            if (!indexFound) {
                System.out.println("No suitable index found, using sequential scan");
                scan = new FileScan(relName, attrTypes, strSizes,
                    (short) relDesc.attrCnt, (short) projlist.length,
                    projlist, expr);
            }
        } else {
            // Use FileScan (no index)
            scan = new FileScan(relName, attrTypes, strSizes,
                (short) relDesc.attrCnt, (short) projlist.length,
                projlist, expr);
        }
        
        // Print header
        System.out.println("Filter query results:");
        System.out.println("--------------------");
        
        // Retrieve and print results
        int resultCount = 0;
        Tuple tuple = null;
        
        while ((tuple = scan.get_next()) != null) {
            System.out.println(tupleToString(tuple, projlist, attrTypes));
            resultCount++;
        }
        
        // Print summary
        System.out.println("--------------------");
        System.out.println("Total records found: " + resultCount);
        
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
 * @throws Exception if there's an error executing the query
 */
private static void executeRangeQuery(String queryExpr, String relName) throws Exception {
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
    
    try {
        if (useIndex) {
            // Try to find an appropriate LSH index for the vector attribute
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
                        
                        // If this index matches our query attribute and is an LSH index
                        if (indexAttrPos == queryAttrNum && 
                            indexDescs[i].accessType.indexType == IndexType.LSHFIndex) {
                            
                            // Get index name - we need to find the actual LSH file
                            // Format is typically: relationName + columnId + "_L" + lValue + "_h" + hValue + ".ser"
                            File dir = new File(".");
                            String prefix = relName + queryAttrNum + "_L";
                            File[] matchingFiles = dir.listFiles((d, name) -> 
                                name.startsWith(prefix) && name.endsWith(".ser"));
                            
                            if (matchingFiles != null && matchingFiles.length > 0) {
                                String indexName = matchingFiles[0].getName();
                                
                                System.out.println("Using LSH index for range query: " + indexName);
                                
                                // Create a null CondExpr array - not needed for RSIndexScan
                                CondExpr[] selects = null;
                                
                                // Create RSIndexScan
                                scan = new RSIndexScan(
                                    new IndexType(IndexType.LSHFIndex),
                                    relName,
                                    indexName,
                                    attrTypes,
                                    strSizes,
                                    relDesc.attrCnt, 
                                    projlist.length,
                                    projlist,
                                    selects,
                                    queryAttrNum, 
                                    targetVec,
                                    rangeDistance
                                );
                                
                                indexFound = true;
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Error accessing index information: " + e.getMessage());
                }
            }
            
            if (!indexFound) {
                System.out.println("No suitable LSH index found, using sequential scan");
                useIndex = false;
            }
        }
        
        // If we couldn't use an index (or chose not to), use sequential scan
        if (scan == null) {
            // Regular sequential scan with manual filtering
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
            // For RSIndexScan, tuples are already filtered by distance
            // For FileScan, we need to filter them manually
            if (!useIndex) {
                // Get vector and calculate distance
                int[] vectorData = tuple.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                double distance = targetVec.distanceTo(vecObj);
                
                // Skip if outside the range
                if (distance > rangeDistance) {
                    continue;
                }
                
                // Print with distance
                System.out.printf("%.2f | %s%n", distance, tupleToString(tuple, projlist, attrTypes));
            } else {
                // For RSIndexScan results, we don't have the distance directly
                // We'd need to recalculate or modify RSIndexScan to return distances
                int[] vectorData = tuple.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                double distance = targetVec.distanceTo(vecObj);
                System.out.printf("%.2f | %s%n", distance, tupleToString(tuple, projlist, attrTypes));
            }
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
 * @throws Exception if there's an error executing the query
 */
private static void executeNNQuery(String queryExpr, String relName) throws Exception {
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
    
    try {
        if (useIndex) {
            // Try to find an appropriate LSH index for the vector attribute
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
                        
                        // If this index matches our query attribute and is an LSH index
                        if (indexAttrPos == queryAttrNum && 
                            indexDescs[i].accessType.indexType == IndexType.LSHFIndex) {
                            
                            // Get index name - find the actual LSH file
                            File dir = new File(".");
                            String prefix = relName + queryAttrNum + "_L";
                            File[] matchingFiles = dir.listFiles((d, name) -> 
                                name.startsWith(prefix) && name.endsWith(".ser"));
                            
                            if (matchingFiles != null && matchingFiles.length > 0) {
                                String indexName = matchingFiles[0].getName();
                                
                                System.out.println("Using LSH index for NN query: " + indexName);
                                
                                // Create NNIndexScan
                                scan = new NNIndexScan(
                                    new IndexType(IndexType.LSHFIndex),
                                    relName,
                                    indexName,
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
                                
                                indexFound = true;
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Error accessing index information: " + e.getMessage());
                }
            }
            
            if (!indexFound) {
                System.out.println("No suitable LSH index found, using sequential scan");
                useIndex = false;
            }
        }
        
        // If no index available or chosen, use sequential scan with manual sorting
        List<Tuple> allTuples = new ArrayList<>();
        List<Double> distances = new ArrayList<>();
        
        if (scan == null) {
            System.out.println("Performing sequential scan for NN query");
            
            // Use FileScan for full relation scan
            scan = new FileScan(relName, attrTypes, strSizes,
                (short) relDesc.attrCnt, (short) projlist.length,
                projlist, null);
                
            // Read all tuples, calculate distances and store them
            Tuple tuple;
            while ((tuple = scan.get_next()) != null) {
                // Make a copy of the tuple
                Tuple tupleCopy = new Tuple(tuple);
                
                // Extract vector and calculate distance
                int[] vectorData = tupleCopy.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                double distance = targetVec.distanceTo(vecObj);
                
                // Store tuple and its distance
                allTuples.add(tupleCopy);
                distances.add(distance);
            }
            
            // Close scan as we've loaded all tuples
            scan.close();
            scan = null;
            
            // Print header
            System.out.println("NN query results (k=" + k + "):");
            System.out.println("---------------------------------------------");
            System.out.println("Distance | Tuple");
            System.out.println("---------------------------------------------");
            
            // Sort indices by corresponding distances
            List<Integer> indices = new ArrayList<>();
            for (int i = 0; i < allTuples.size(); i++) {
                indices.add(i);
            }
            indices.sort(Comparator.comparing(distances::get));
            
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
        } 
        else {
            // Print header for indexed search
            System.out.println("NN query results (k=" + k + "):");
            System.out.println("---------------------------------------------");
            System.out.println("Distance | Tuple");
            System.out.println("---------------------------------------------");
            
            // Process indexed results (already limited to k nearest neighbors)
            int resultCount = 0;
            Tuple tuple;
            
            while ((tuple = scan.get_next()) != null) {
                // For NNIndexScan results, we need to manually calculate distance for display
                int[] vectorData = tuple.getVectorFld(queryAttrNum);
                Vector100Dtype vecObj = new Vector100Dtype(vectorData);
                double distance = targetVec.distanceTo(vecObj);
                
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
        // Close the scan if still open
        if (scan != null) {
            scan.close();
        }
    }
}

private static void executeDJoinQuery(String queryExpr, String relName1, String relName2) throws Exception {
    String[] params = parseQueryParams(queryExpr);
    
    if (params.length < 4) {
        throw new Exception("DJOIN query requires at least 4 parameters: QA1, QA2, D, I, [output fields]");
    }
    
    // Parse parameters
    int queryAttr1 = Integer.parseInt(params[0].trim());
    int queryAttr2 = Integer.parseInt(params[1].trim());
    int distance = Integer.parseInt(params[2].trim());
    String indexOption = params[3].trim();
    
    if (distance < 0) {
        throw new Exception("Distance Join threshold D must be non-negative");
    }
    
    // Parse output fields
    String[] outputFields = null;
    if (params.length > 4) {
        String outputSpec = params[4].trim();
        if (!outputSpec.equals("*")) {
            outputFields = outputSpec.split("\\s+");
        }
    }
    
    // Get relation information for both relations
    RelDesc relDesc1 = new RelDesc();
    RelDesc relDesc2 = new RelDesc();
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName1, relDesc1);
    ExtendedSystemDefs.MINIBASE_RELCAT.getInfo(relName2, relDesc2);
    
    // Get attribute information for both relations
    AttrDesc[] attrDescs1 = new AttrDesc[relDesc1.attrCnt];
    AttrDesc[] attrDescs2 = new AttrDesc[relDesc2.attrCnt];
    for (int i = 0; i < relDesc1.attrCnt; i++) {
        attrDescs1[i] = new AttrDesc();
    }
    for (int i = 0; i < relDesc2.attrCnt; i++) {
        attrDescs2[i] = new AttrDesc();
    }
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName1, relDesc1.attrCnt, attrDescs1);
    ExtendedSystemDefs.MINIBASE_ATTRCAT.getRelInfo(relName2, relDesc2.attrCnt, attrDescs2);
    
    // Verify both query attributes are vectors
    if (queryAttr1 < 1 || queryAttr1 > relDesc1.attrCnt) {
        throw new Exception("Invalid query attribute number for relation 1: " + queryAttr1);
    }
    if (queryAttr2 < 1 || queryAttr2 > relDesc2.attrCnt) {
        throw new Exception("Invalid query attribute number for relation 2: " + queryAttr2);
    }
    
    if (attrDescs1[queryAttr1-1].attrType.attrType != AttrType.attrVector100D) {
        throw new Exception("Distance Join attribute for relation 1 must be a vector");
    }
    if (attrDescs2[queryAttr2-1].attrType.attrType != AttrType.attrVector100D) {
        throw new Exception("Distance Join attribute for relation 2 must be a vector");
    }
    
    // Setup query attributes for both relations
    AttrType[] attrTypes1 = new AttrType[relDesc1.attrCnt];
    AttrType[] attrTypes2 = new AttrType[relDesc2.attrCnt];
    int strCount1 = 0, strCount2 = 0;
    
    for (int i = 0; i < relDesc1.attrCnt; i++) {
        attrTypes1[i] = attrDescs1[i].attrType;
        if (attrTypes1[i].attrType == AttrType.attrString) {
            strCount1++;
        }
    }
    
    for (int i = 0; i < relDesc2.attrCnt; i++) {
        attrTypes2[i] = attrDescs2[i].attrType;
        if (attrTypes2[i].attrType == AttrType.attrString) {
            strCount2++;
        }
    }
    
    // Create string sizes arrays
    short[] strSizes1 = new short[strCount1];
    short[] strSizes2 = new short[strCount2];
    int strIndex1 = 0, strIndex2 = 0;
    
    for (int i = 0; i < relDesc1.attrCnt; i++) {
        if (attrTypes1[i].attrType == AttrType.attrString) {
            strSizes1[strIndex1++] = (short) attrDescs1[i].attrLen;
        }
    }
    
    for (int i = 0; i < relDesc2.attrCnt; i++) {
        if (attrTypes2[i].attrType == AttrType.attrString) {
            strSizes2[strIndex2++] = (short) attrDescs2[i].attrLen;
        }
    }
    
    // Setup projection list
    FldSpec[] projList;
    int outFieldCount;
    
    if (outputFields == null) {
        // Use all fields from both relations
        outFieldCount = relDesc1.attrCnt + relDesc2.attrCnt;
        projList = new FldSpec[outFieldCount];
        
        // Fields from outer relation (rel1)
        for (int i = 0; i < relDesc1.attrCnt; i++) {
            projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
        }
        
        // Fields from inner relation (rel2)
        for (int i = 0; i < relDesc2.attrCnt; i++) {
            projList[relDesc1.attrCnt + i] = new FldSpec(new RelSpec(RelSpec.innerRel), i+1);
        }
    } else {
        // Parse field specifications with format O# or I# for outer/inner relation fields
        List<FldSpec> projections = new ArrayList<>();
        
        for (String fieldSpec : outputFields) {
            fieldSpec = fieldSpec.trim().toUpperCase();
            
            if (fieldSpec.length() < 2) {
                throw new Exception("Invalid field specification: " + fieldSpec);
            }
            
            char relType = fieldSpec.charAt(0);
            int fieldNum;
            
            try {
                fieldNum = Integer.parseInt(fieldSpec.substring(1));
            } catch (NumberFormatException e) {
                throw new Exception("Invalid field number in: " + fieldSpec);
            }
            
            if (relType == 'O') {
                // Outer relation field
                if (fieldNum < 1 || fieldNum > relDesc1.attrCnt) {
                    throw new Exception("Invalid field number for outer relation: " + fieldNum);
                }
                projections.add(new FldSpec(new RelSpec(RelSpec.outer), fieldNum));
            } else if (relType == 'I') {
                // Inner relation field
                if (fieldNum < 1 || fieldNum > relDesc2.attrCnt) {
                    throw new Exception("Invalid field number for inner relation: " + fieldNum);
                }
                projections.add(new FldSpec(new RelSpec(RelSpec.innerRel), fieldNum));
            } else {
                throw new Exception("Invalid relation specifier: " + relType + ". Must be 'O' or 'I'");
            }
        }
        
        outFieldCount = projections.size();
        if (outFieldCount == 0) {
            throw new Exception("No valid output fields specified");
        }
        projList = projections.toArray(new FldSpec[outFieldCount]);
    }
    
    // Create outer relation scan
    FileScan outerScan = new FileScan(relName1, attrTypes1, strSizes1,
                                     (short)relDesc1.attrCnt, (short)relDesc1.attrCnt,
                                     null, null);
    
    // Determine if we should use index
    boolean useIndex = indexOption.equalsIgnoreCase("H");
    
    // Create join conditions for vector distance join
    CondExpr[] joinExpr = new CondExpr[2]; // One for condition, one for null terminator
    joinExpr[0] = new CondExpr();
    joinExpr[0].op = new AttrOperator(AttrOperator.aopVECTORDIST);
    joinExpr[0].type1 = new AttrType(AttrType.attrSymbol);
    joinExpr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), queryAttr1);
    joinExpr[0].type2 = new AttrType(AttrType.attrSymbol);
    joinExpr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), queryAttr2);
    joinExpr[0].distance = distance;
    joinExpr[1] = null; // End of expression list
    
    iterator.Iterator join = null;
    String indexName = null;
    
    try {
        // If using index, try to find a suitable LSH index for relation 2
        if (useIndex) {
            boolean indexFound = false;
            int indexCount = relDesc2.indexCnt;
            
            if (indexCount > 0) {
                IndexDesc[] indexDescs = new IndexDesc[indexCount];
                for (int i = 0; i < indexCount; i++) {
                    indexDescs[i] = new IndexDesc();
                }
                
                ExtendedSystemDefs.MINIBASE_INDCAT.getRelInfo(relName2, indexCount, indexDescs);
                
                // Find a suitable LSH index for the vector attribute in relation 2
                for (int i = 0; i < indexCount; i++) {
                    // Find the attribute position for this index
                    int indexAttrPos = -1;
                    for (int j = 0; j < relDesc2.attrCnt; j++) {
                        if (attrDescs2[j].attrName.equals(indexDescs[i].attrName)) {
                            indexAttrPos = j + 1; // Convert to 1-based
                            break;
                        }
                    }
                    
                    // If this index matches our query attribute and is an LSH index
                    if (indexAttrPos == queryAttr2 && 
                        indexDescs[i].accessType.indexType == IndexType.LSHFIndex) {
                        
                        // Find the actual LSH file
                        File dir = new File(".");
                        String prefix = relName2 + queryAttr2 + "_L";
                        File[] matchingFiles = dir.listFiles((d, name) -> 
                            name.startsWith(prefix) && name.endsWith(".ser"));
                        
                        if (matchingFiles != null && matchingFiles.length > 0) {
                            indexName = matchingFiles[0].getName();
                            indexFound = true;
                            System.out.println("Using LSH index for Distance Join: " + indexName);
                            break;
                        }
                    }
                }
            }
            
            if (!indexFound) {
                System.out.println("No suitable LSH index found for relation 2, using nested loop join");
                useIndex = false;
            }
        }
        
        // Create appropriate join operator
        if (useIndex && indexName != null) {
            // Create an INLJoins using the LSH index
            join = new INLJoins(
                attrTypes1, relDesc1.attrCnt, strSizes1,
                attrTypes2, relDesc2.attrCnt, strSizes2,
                10, // Memory buffer pages
                outerScan,
                relName2,
                new IndexType(IndexType.LSHFIndex),
                indexName,
                joinExpr,
                null, // No additional right filter
                projList,
                outFieldCount
            );
        } else {
            // Use nested loop join without index
            join = new NestedLoopsJoins(
                attrTypes1, relDesc1.attrCnt, strSizes1,
                attrTypes2, relDesc2.attrCnt, strSizes2,
                10, // Memory buffer pages
                outerScan,
                relName2,
                joinExpr,
                null, // No right filter
                projList,
                outFieldCount
            );
        }
        
        // Create combined output attribute type array
        AttrType[] jTypes = new AttrType[outFieldCount];
        
        for (int i = 0; i < outFieldCount; i++) {
            if (projList[i].relation.key == RelSpec.outer) {
                jTypes[i] = attrTypes1[projList[i].offset - 1];
            } else {
                jTypes[i] = attrTypes2[projList[i].offset - 1];
            }
        }
        
        // Process and display results
        System.out.println("Distance Join results (max distance: " + distance + "):");
        System.out.println("---------------------------------------------");
        System.out.println("Tuple");
        System.out.println("---------------------------------------------");
        
        // Retrieve and display joined tuples
        Tuple tuple;
        int resultCount = 0;
        
        while ((tuple = join.get_next()) != null) {
            System.out.println(tupleToString(tuple, projList, jTypes));
            resultCount++;
        }
        
        System.out.println("---------------------------------------------");
        System.out.println("Total records found: " + resultCount);
    } finally {
        // Close the join operator
        if (join != null) {
            join.close();
        } else if (outerScan != null) {
            // If join wasn't created, still need to close the outer scan
            outerScan.close();
        }
    }
}
    
}