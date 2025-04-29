//------------------------------------
// RelDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

import java.io.IOException;
import global.AttrType;
import global.Catalogglobal;
import heap.Tuple;
import heap.FieldNumberOutOfBoundException;

//   RelDesc class: schema of relation catalog:
public class RelDesc
{
	public String relName;                   // relation name
	public int  attrCnt = 0;                 // number of attributes
	public int  indexCnt = 0;                // number of indexed attrs
	public int  numTuples = 0;               // number of tuples in the relation
	public int  numPages = 0;                // number of pages in the file

    public void tupleToRelDesc(Tuple tuple)
        throws IOException, FieldNumberOutOfBoundException {

        // Create a temporary tuple to set the header, allowing field access
        // The schema matches the one defined in Catalog.initialize() for relcatalog
        AttrType[] types = new AttrType[5];
        types[0] = new AttrType(AttrType.attrString);
        types[1] = new AttrType(AttrType.attrInteger);
        types[2] = new AttrType(AttrType.attrInteger);
        types[3] = new AttrType(AttrType.attrInteger);
        types[4] = new AttrType(AttrType.attrInteger);

        short[] sizes = new short[1]; // For string size
        sizes[0] = Catalogglobal.MAXNAME; // Use MAXNAME from Catalogglobal

        // Set the header information on the tuple
        // We need the data array, offset, and length from the input tuple
        try {
             tuple.setHdr((short) 5, types, sizes);
        } catch (Exception e) {
             // Handle potential exceptions from setHdr, e.g., InvalidTypeException
             throw new IOException("Error setting tuple header in tupleToRelDesc: " + e);
        }


        // Extract fields using the header info
        // Field numbers are 1-based
        this.relName = tuple.getStrFld(1);
        this.attrCnt = tuple.getIntFld(2);
        this.indexCnt = tuple.getIntFld(3);
        this.numTuples = tuple.getIntFld(4);
        this.numPages = tuple.getIntFld(5);
    }


};

