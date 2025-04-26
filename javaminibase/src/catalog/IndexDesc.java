//------------------------------------
// IndexDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

import global.*;
import diskmgr.*;
import bufmgr.*;

// IndexDesc class: schema for index catalog
public class IndexDesc
{
    public 	String relName;                     // relation name
	public String attrName;                    // attribute name
	public IndexType  accessType;                // access method
	public TupleOrder order;                     // order of keys
	public int        clustered = 0;                 //
	public int        distinctKeys = 0;              // no of distinct key values
	public int        indexPages = 0;                // no of index pages
};

