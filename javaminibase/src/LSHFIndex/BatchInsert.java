package LSHFIndex;

import global.*;
import heap.*;
import LSHFIndex.*;
import java.io.*;
import java.util.*;
import diskmgr.PCounter; // Ensure this is the correct package for PCounter

public class BatchInsert {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: batchinsert <h> <L> <DATAFILENAME> <DBNAME>");
            return;
        }

        int h = Integer.parseInt(args[0]);
        int L = Integer.parseInt(args[1]);
        String dataFileName = args[2];
        String dbName = args[3];

        try {
            // Open the data file
            BufferedReader reader = new BufferedReader(new FileReader(dataFileName));

            // Read the number of attributes
            int numAttributes = Integer.parseInt(reader.readLine().trim());

            // Read the attribute types
            String[] attributeTypes = reader.readLine().trim().split(" ");
            AttrType[] attrTypes = new AttrType[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                int type = Integer.parseInt(attributeTypes[i]);
                switch (type) {
                    case 1: attrTypes[i] = new AttrType(AttrType.attrInteger); break;
                    case 2: attrTypes[i] = new AttrType(AttrType.attrReal); break;
                    case 3: attrTypes[i] = new AttrType(AttrType.attrString); break;
                    case 4: attrTypes[i] = new AttrType(AttrType.attrVector100D); break;
                    default: throw new IllegalArgumentException("Invalid attribute type: " + type);
                }
            }

            // Create a heap file for the database
            Heapfile heapfile = new Heapfile(dbName);

            // Read and insert tuples
            String line;
            while ((line = reader.readLine()) != null) {
                Tuple tuple = new Tuple();
                tuple.setHdr((short) numAttributes, attrTypes, null);

                // Set the fields of the tuple
                for (int i = 0; i < numAttributes; i++) {
                    switch (attrTypes[i].attrType) {
                        case AttrType.attrInteger:
                            tuple.setIntFld(i + 1, Integer.parseInt(line.trim()));
                            break;
                        case AttrType.attrReal:
                            tuple.setFloFld(i + 1, Float.parseFloat(line.trim()));
                            break;
                        case AttrType.attrString:
                            tuple.setStrFld(i + 1, line.trim());
                            break;
                        case AttrType.attrVector100D:
                            String[] vectorValues = line.trim().split(" ");
                            int[] vector = Arrays.stream(vectorValues).mapToInt(Integer::parseInt).toArray();
                            tuple.setVectorFld(i + 1, vector);
                            break;
                    }
                    line = reader.readLine(); // Read the next line for the next field
                }

                // Insert the tuple into the heap file
                heapfile.insertRecord(tuple.getTupleByteArray());
            }

            // Create LSH-forest indexes for attrVector100D attributes
            for (int i = 0; i < numAttributes; i++) {
                if (attrTypes[i].attrType == AttrType.attrVector100D) {
                    String indexName = dbName + "_attr" + (i + 1) + "_h" + h + "_L" + L;
                    LSHFIndex index = new LSHFIndex(h, L);

                    // Scan the heap file and insert records into the index
                    Scan scan = heapfile.openScan();
                    RID rid = new RID();
                    Tuple temp = new Tuple();
                    while ((temp = scan.getNext(rid)) != null) {
                        temp.setHdr((short) numAttributes, attrTypes, null);
                        int[] vectorArray = temp.getVectorFld(i + 1);
                        Vector100Dtype vector = new Vector100Dtype(vectorArray);
                        Vector100DKey vectorKey = new Vector100DKey(vector);
                        index.insert(vectorKey, rid);
                    }
                    scan.closescan();
                    
                    String indexFilePath = indexName + ".ser";
                    index.saveIndex(indexFilePath);
                    System.out.println("Index saved to: " + indexFilePath);
                }
            }

            // Output disk page statistics
            System.out.println("Disk pages read: " + PCounter.getRCount());
            System.out.println("Disk pages written: " + PCounter.getWCount());

            // Add this code at the end of BatchInsert's main method, just before closing

            // Save schema information to a companion file
            try (FileWriter schemaWriter = new FileWriter(dbName + ".schema")) {
                // Write number of attributes
                schemaWriter.write(Integer.toString(numAttributes) + "\n");
                
                // Write attribute types
                for (int i = 0; i < numAttributes; i++) {
                    int typeCode = 0;
                    switch (attrTypes[i].attrType) {
                        case AttrType.attrInteger: typeCode = 1; break;
                        case AttrType.attrReal: typeCode = 2; break;
                        case AttrType.attrString: typeCode = 3; break;
                        case AttrType.attrVector100D: typeCode = 4; break;
                    }
                    schemaWriter.write(typeCode + " ");
                }
                
                // Write string lengths if needed
                schemaWriter.write("\n");
                for (int i = 0; i < numAttributes; i++) {
                    if (attrTypes[i].attrType == AttrType.attrString) {
                        // Write the string size (assumed to be 30 if not specified)
                        schemaWriter.write("30 ");
                    } else {
                        schemaWriter.write("0 ");
                    }
                }
                
                System.out.println("Schema file created: " + dbName + ".schema");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}