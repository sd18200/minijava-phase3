/*  File RID.java   */

package global;

import java.io.*;
import java.util.Objects;

/** class RID
 */

public class RID implements Serializable{
  
  /** public int slotNo
   */
  public int slotNo;
  
  /** public PageId pageNo
   */
  public PageId pageNo = new PageId();
  
  /**
   * default constructor of class
   */
  public RID () { }
  
  /**
   *  constructor of class
   */
  public RID (PageId pageno, int slotno)
    {
      pageNo = pageno;
      slotNo = slotno;
    }
  
  /**
   * make a copy of the given rid
   */
  public void copyRid (RID rid)
    {
      pageNo = rid.pageNo;
      slotNo = rid.slotNo;
    }
  
  /** Write the rid into a byte array at offset
   * @param ary the specified byte array
   * @param offset the offset of byte array to write 
   * @exception java.io.IOException I/O errors
   */ 
  public void writeToByteArray(byte [] ary, int offset)
    throws java.io.IOException
    {
      Convert.setIntValue ( slotNo, offset, ary);
      Convert.setIntValue ( pageNo.pid, offset+4, ary);
    }
  
  
  /** Compares two RID object, i.e, this to the rid
   * @param rid RID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  @Override // Add Override annotation
  public boolean equals(Object obj) {
    // 1. Check for self comparison
    if (this == obj) return true;

    // 2. Check for null and correct type
    if (obj == null || getClass() != obj.getClass()) return false;

    // 3. Cast to RID
    RID other = (RID) obj;

    // 4. Compare fields (handle potential null pageNo)
    // Use Objects.equals for null-safe comparison of pageNo.pid
    boolean pageEquals = (this.pageNo == null && other.pageNo == null) ||
                         (this.pageNo != null && other.pageNo != null && this.pageNo.pid == other.pageNo.pid);

    return pageEquals && this.slotNo == other.slotNo;
  }

    @Override // Add Override annotation
  public int hashCode() {
    // Use Objects.hash for a convenient way to generate hash code
    // Include pid from pageNo (handle null pageNo)
    int pagePid = (pageNo != null) ? pageNo.pid : 0; // Use 0 or another default if pageNo is null
    return Objects.hash(pagePid, slotNo);
  }

  /**
   * Returns a string representation of the RID.
   * @return String representation like "[pageNo, slotNo]"
   */
  @Override
  public String toString() {
      return "[" + ((pageNo != null) ? pageNo.pid : "null") + ", " + slotNo + "]";
  }
  
}
