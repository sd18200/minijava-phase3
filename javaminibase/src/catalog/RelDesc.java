//------------------------------------
// RelDesc.java
//
// Ning Wang, April,24  1998
//-------------------------------------

package catalog;

//   RelDesc class: schema of relation catalog:
public class RelDesc
{
	public String relName;                   // relation name
	public int  attrCnt = 0;                 // number of attributes
	public int  indexCnt = 0;                // number of indexed attrs
	public int  numTuples = 0;               // number of tuples in the relation
	public int  numPages = 0;                // number of pages in the file
};

