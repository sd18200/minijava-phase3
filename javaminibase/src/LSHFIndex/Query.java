package LSHFIndex;

import global.*;
import heap.*;
import iterator.*;
import java.io.*;
import java.util.*;
import diskmgr.PCounter;
import iterator.Iterator;


public class Query {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: query <DBNAME> <QSNAME> <INDEXOPTION> <NUMBUF>");
            return;
        }

        String dbName = args[0];
        String qsName = args[1];
        String indexOption = args[2];
        int numBuf = Integer.parseInt(args[3]);

        try {
            // Configure the buffer manager
            SystemDefs sysdef = new SystemDefs(dbName, 0, numBuf, "Clock");

            // Parse the query specification file
            BufferedReader reader = new BufferedReader(new FileReader(qsName));
            String queryType = reader.readLine().trim();
            String[] queryParams = queryType.substring(queryType.indexOf('(') + 1, queryType.indexOf(')')).split(",");
            int qa = Integer.parseInt(queryParams[0].trim());
            String targetFile = queryParams[1].trim();
            Vector100Dtype target = loadTargetVector(targetFile);

            // Load schema dynamically from file
            AttrType[] types = loadSchemaFromFile(dbName);

            int[] outputFields;
            if (queryParams[queryParams.length - 1].trim().equals("*")) {
                // Include all fields if * is specified
                outputFields = new int[types.length];
                for (int i = 0; i < outputFields.length; i++) {
                    outputFields[i] = i + 1; // Field numbers are 1-based
                }
            } else {
                // Parse specific output fields
                outputFields = Arrays.stream(queryParams, 3, queryParams.length)
                                     .mapToInt(Integer::parseInt)
                                     .toArray();
            }

            if (queryType.startsWith("Range")) {
                int distance = Integer.parseInt(queryParams[2].trim());
                executeRangeQuery(dbName, qa, target, distance, indexOption, outputFields);
            } else if (queryType.startsWith("NN")) {
                int k = Integer.parseInt(queryParams[2].trim());
                executeNNQuery(dbName, qa, target, k, indexOption, outputFields);
            }

            // Output disk page statistics
            System.out.println("Disk pages read: " + PCounter.getRCount());
            System.out.println("Disk pages written: " + PCounter.getWCount());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Vector100Dtype loadTargetVector(String targetFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(targetFile));
        String[] vectorValues = reader.readLine().trim().split(" ");
        int[] vector = Arrays.stream(vectorValues).mapToInt(Integer::parseInt).toArray();
        reader.close();
        return new Vector100Dtype(vector);
    }

    private static void executeRangeQuery(String dbName, int qa, Vector100Dtype target, int distance, String indexOption,int[] outputFields) throws Exception {
        // Load schema dynamically
        AttrType[] types = loadSchemaFromFile(dbName);
        short[] str_sizes = loadStringSizes(dbName);
        
        if (indexOption.equals("Y")) {
            String indName = findIndexFile(dbName, qa);
            if (indName == null) {
                System.err.println("Error: No index file found for " + dbName + " attribute " + qa);
                System.err.println("Falling back to sequential scan");
                indexOption = "N"; // Fall back to sequential scan
            } else {
                FldSpec[] outFlds = defineOutputFields(outputFields);
                CondExpr[] selects = defineSelectionConditions();

                // Create RSIndexScan
                RSIndexScan scan = new RSIndexScan(
                    new IndexType(IndexType.LSHFIndex),
                    dbName,
                    indName,
                    types,
                    str_sizes,
                    types.length,
                    outFlds.length,
                    outFlds,
                    selects,
                    qa,
                    target,
                    distance
                );
                printResults(scan);
                return;
            }
        }
        
        // Sequential scan fallback
        // Perform a full scan of the heap file and filter results manually
        Heapfile heapfile = new Heapfile(dbName);
        Scan scan = heapfile.openScan();
        RID rid = new RID();
        Tuple tuple = new Tuple();
        tuple.setHdr((short) types.length, types, str_sizes);

        List<Tuple> results = new ArrayList<>();
        while ((tuple = scan.getNext(rid)) != null) {
            tuple.setHdr((short) types.length, types, str_sizes);
            int[] vector = tuple.getVectorFld(qa); // Get the vector field
            Vector100Dtype tupleVector = new Vector100Dtype(vector);
        
            // Compute the distance
            double distanceToTarget = target.distanceTo(tupleVector);
            if (distanceToTarget <= distance) {
                // Create a new tuple with only the specified output fields
                Tuple resultTuple = new Tuple();
                resultTuple.setHdr((short) outputFields.length, types, str_sizes);
                for (int i = 0; i < outputFields.length; i++) {
                    int fieldNo = outputFields[i];
                    switch (types[fieldNo - 1].attrType) {
                        case AttrType.attrInteger:
                            resultTuple.setIntFld(i + 1, tuple.getIntFld(fieldNo));
                            break;
                        case AttrType.attrString:
                            resultTuple.setStrFld(i + 1, tuple.getStrFld(fieldNo));
                            break;
                        case AttrType.attrVector100D:
                            int[] fieldVector = tuple.getVectorFld(fieldNo);
                            resultTuple.setVectorFld(i + 1, fieldVector);
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported field type");
                    }
                }
                results.add(resultTuple);
            }
        }
        scan.closescan();

        // Print the results
        for (Tuple result : results) {
            System.out.println(result);
        }
    }

    private static void executeNNQuery(String dbName, int qa, Vector100Dtype target, int k, String indexOption,int[] outputFields) throws Exception {
        // Load schema dynamically
        AttrType[] types = loadSchemaFromFile(dbName);
        short[] str_sizes = loadStringSizes(dbName);
        
        if (indexOption.equals("Y")) {
            String indName = findIndexFile(dbName, qa);
            if (indName == null) {
                System.err.println("Error: No index file found for " + dbName + " attribute " + qa);
                System.err.println("Falling back to sequential scan");
                indexOption = "N"; // Fall back to sequential scan
            } else {
                FldSpec[] outFlds = defineOutputFields(outputFields);
                CondExpr[] selects = defineSelectionConditions();

                // Create NNIndexScan
                NNIndexScan scan = new NNIndexScan(
                    new IndexType(IndexType.LSHFIndex),
                    dbName,
                    indName,
                    types,
                    str_sizes,
                    types.length,
                    outFlds.length,
                    outFlds,
                    selects,
                    qa,
                    target,
                    k
                );
                printResults(scan);
                return;
            }
        }
        
        // Sequential scan fallback
        // Perform a full scan of the heap file and find nearest neighbors manually
        Heapfile heapfile = new Heapfile(dbName);
        Scan scan = heapfile.openScan();
        RID rid = new RID();
        Tuple tuple = new Tuple();
        tuple.setHdr((short) types.length, types, str_sizes);

        PriorityQueue<RIDDistancePair> pq = new PriorityQueue<>(Comparator.comparingDouble(pair -> -pair.distance));
        while ((tuple = scan.getNext(rid)) != null) {
            tuple.setHdr((short) types.length, types, str_sizes);
            int[] vector = tuple.getVectorFld(qa); // Get the vector field
            Vector100Dtype tupleVector = new Vector100Dtype(vector);
        
            // Compute the distance
            double distanceToTarget = target.distanceTo(tupleVector);
            
            if (k == 0 || pq.size() < k) {
                // Add all entries if k=0 or we don't have k entries yet
                pq.add(new RIDDistancePair(new RID(rid.pageNo, rid.slotNo), distanceToTarget));
            } else if (distanceToTarget < -pq.peek().distance) {
                // Replace farthest if this one is closer
                pq.poll();
                pq.add(new RIDDistancePair(new RID(rid.pageNo, rid.slotNo), distanceToTarget));
            }
        }
        scan.closescan();

        // Create list of results in correct order (ascending distance)
        List<RIDDistancePair> results = new ArrayList<>();
        while (!pq.isEmpty()) {
            RIDDistancePair pair = pq.poll();
            pair.distance = -pair.distance; // Fix negated distance
            results.add(0, pair); // Add at front to reverse order
        }

        // Print the results
        for (RIDDistancePair pair : results) {
            tuple = heapfile.getRecord(pair.rid);
            tuple.setHdr((short) types.length, types, str_sizes);
            
            // Create a new tuple with only the specified output fields
            Tuple resultTuple = new Tuple();
            resultTuple.setHdr((short) outputFields.length, types, str_sizes);
            for (int i = 0; i < outputFields.length; i++) {
                int fieldNo = outputFields[i];
                switch (types[fieldNo - 1].attrType) {
                    case AttrType.attrInteger:
                        resultTuple.setIntFld(i + 1, tuple.getIntFld(fieldNo));
                        break;
                    case AttrType.attrString:
                        resultTuple.setStrFld(i + 1, tuple.getStrFld(fieldNo));
                        break;
                    case AttrType.attrVector100D:
                        int[] fieldVector = tuple.getVectorFld(fieldNo);
                        resultTuple.setVectorFld(i + 1, fieldVector);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported field type");
                }
            }
            System.out.println("Distance: " + pair.distance + " - " + resultTuple);
        }
    }

    private static void printResults(Iterator scan) throws Exception {
        Tuple tuple;
        while ((tuple = scan.get_next()) != null) {
            System.out.println(tuple);
        }
        scan.close();
    }

    // Original hardcoded schema methods (kept as fallbacks)
    private static AttrType[] defineAttrTypes() {
        return new AttrType[] {
            new AttrType(AttrType.attrInteger),      // Attribute 1: Integer
            new AttrType(AttrType.attrString),       // Attribute 2: String
            new AttrType(AttrType.attrVector100D)    // Attribute 3: 100D Vector
        };
    }

    private static short[] defineStringSizes() {
        return new short[] { 20 }; // Attribute 2: String of size 20
    }

    // New dynamic schema loading methods
    private static AttrType[] loadSchemaFromFile(String dbName) throws IOException {
        File schemaFile = new File(dbName + ".schema");
        
        if (!schemaFile.exists()) {
            System.out.println("Warning: Schema file not found. Using default schema.");
            return defineAttrTypes(); // Fallback to hardcoded schema
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            // Read number of attributes
            int numAttributes = Integer.parseInt(reader.readLine().trim());
            
            // Read attribute types
            String[] typeStrs = reader.readLine().trim().split(" ");
            AttrType[] attrTypes = new AttrType[numAttributes];
            
            for (int i = 0; i < numAttributes; i++) {
                int typeCode = Integer.parseInt(typeStrs[i]);
                switch (typeCode) {
                    case 1: attrTypes[i] = new AttrType(AttrType.attrInteger); break;
                    case 2: attrTypes[i] = new AttrType(AttrType.attrReal); break;
                    case 3: attrTypes[i] = new AttrType(AttrType.attrString); break;
                    case 4: attrTypes[i] = new AttrType(AttrType.attrVector100D); break;
                    default: throw new IOException("Invalid attribute type code: " + typeCode);
                }
            }
            
            return attrTypes;
        }
    }

    private static short[] loadStringSizes(String dbName) throws IOException {
        File schemaFile = new File(dbName + ".schema");
        
        if (!schemaFile.exists()) {
            System.out.println("Warning: Schema file not found. Using default string sizes.");
            return defineStringSizes(); // Fallback to hardcoded sizes
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(schemaFile))) {
            // Read number of attributes
            reader.readLine();
            
            // Read attribute types
            String[] typeStrs = reader.readLine().trim().split(" ");
            int numAttributes = typeStrs.length;
            
            // Read string sizes
            String[] sizeStrs = reader.readLine().trim().split(" ");
            
            // Count how many string fields we have
            int stringCount = 0;
            for (int i = 0; i < numAttributes; i++) {
                if (Integer.parseInt(typeStrs[i]) == 3) { // If type is string
                    stringCount++;
                }
            }
            
            if (stringCount == 0) {
                // No string fields, return empty array
                return new short[0];
            }
            
            // Create array of only the string sizes (non-zero values)
            short[] strSizes = new short[stringCount];
            int index = 0;
            for (int i = 0; i < numAttributes; i++) {
                if (Integer.parseInt(typeStrs[i]) == 3) { // If type is string
                    strSizes[index++] = Short.parseShort(sizeStrs[i]);
                }
            }
            
            return strSizes;
        }
    }

    private static FldSpec[] defineOutputFields(int[] fieldNumbers) {
        FldSpec[] outFlds = new FldSpec[fieldNumbers.length];
        for (int i = 0; i < fieldNumbers.length; i++) {
            outFlds[i] = new FldSpec(new RelSpec(RelSpec.outer), fieldNumbers[i]);
        }
        return outFlds;
    }

    private static CondExpr[] defineSelectionConditions() {
        CondExpr[] selects = new CondExpr[2]; // One condition + null terminator
        selects[0] = new CondExpr();
        selects[0].type1 = new AttrType(AttrType.attrInteger); // Attribute type
        selects[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1); // Field 1
        selects[0].op = new AttrOperator(AttrOperator.aopGT); // Greater than
        selects[0].type2 = new AttrType(AttrType.attrInteger); // Constant type
        selects[0].operand2.integer = 10; // Compare against 10
        selects[1] = null; // Null terminator
        return selects;
    }

    private static String findIndexFile(String dbName, int attributeNumber) {
        File directory = new File(".");
        String prefix = dbName + "_attr" + attributeNumber;
        
        // Look for files matching the pattern: dbName_attrX_h*_L*.ser
        File[] files = directory.listFiles((dir, name) -> 
            name.startsWith(prefix) && name.contains("_h") && name.contains("_L") && name.endsWith(".ser"));
        
        if (files == null || files.length == 0) {
            return null;
        }
        
        // Return just the filename without .ser extension
        return files[0].getName().substring(0, files[0].getName().length() - 4);
    }
}