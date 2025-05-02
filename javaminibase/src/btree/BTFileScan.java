/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package btree;
import java.io.*;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.*;

/**
 * BTFileScan implements a search/iterate interface to B+ tree 
 * index files (class BTreeFile).  It derives from abstract base
 * class IndexFileScan.  
 */
public class BTFileScan  extends IndexFileScan
             implements  GlobalConst
{

  BTreeFile bfile; 
  String treeFilename;     // B+ tree we're scanning 
  BTLeafPage leafPage;   // leaf page containing current record
  RID curRid;       // position in current leaf; note: this is 
                             // the RID of the key/RID pair within the
                             // leaf page.                                    
  boolean didfirst;        // false only before getNext is called
  boolean deletedcurrent;  // true after deleteCurrent is called (read
                           // by get_next, written by deleteCurrent).
    
  KeyClass endkey;    // if NULL, then go all the way right
                        // else, stop when current record > this value.
                        // (that is, implement an inclusive range 
                        // scan -- the only way to do a search for 
                        // a single value).
  int keyType;
  int maxKeysize;

  /**
   * Iterate once (during a scan).  
   *@return null if done; otherwise next KeyDataEntry
   *@exception ScanIteratorException iterator error
   */
  public KeyDataEntry get_next()
    throws ScanIteratorException
  {

    KeyDataEntry entry;
    PageId nextpage;
    try {
      if (leafPage == null) {
        return null;
      }

      // Determine if it's the first call or after a delete
      if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
         didfirst = true;
         deletedcurrent = false;
         entry=leafPage.getCurrent(curRid);
         if (entry == null) {
         } else {
         }
      }
      else {
         entry = leafPage.getNext(curRid);
          if (entry == null) {
         } else {
         }
      }

      // Loop if entry is null (e.g., end of current page)
      while ( entry == null ) {
         nextpage = leafPage.getNextPage();
         try {
             // --- FIX: Unpin read-only page (dirty=false) ---
             SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false); // Use false for dirty flag
         } catch (Exception eUnpin) {
              System.err.println("ERROR: BTFileScan.get_next(): Exception unpinning page " + leafPage.getCurPage().pid + ": " + eUnpin.getMessage());
              // --- FIX: Use correct constructor ---
              throw new ScanIteratorException("Failed to unpin page in get_next: " + eUnpin.getMessage());
         }

         if (nextpage.pid == INVALID_PAGE) {
            leafPage = null; // Set leafPage to null as scan ends
            return null;
         }

         leafPage=new BTLeafPage(nextpage, keyType); // Pin and create new leaf page object

         entry=leafPage.getFirst(curRid);
         if (entry == null) {
         } else {
         }
      } // End while (entry == null)

      // Check against endkey if provided
      if (endkey != null) {
        if ( BT.keyCompare(entry.key, endkey)  > 0) {
            // went past right end of scan
            try {
                // --- FIX: Unpin read-only page (dirty=false) ---
                SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false); // Use false for dirty flag
            } catch (Exception eUnpin) {
                 System.err.println("ERROR: BTFileScan.get_next(): Exception unpinning page " + leafPage.getCurPage().pid + " after endkey check: " + eUnpin.getMessage());
                 // --- FIX: Use correct constructor ---
                 throw new ScanIteratorException("Failed to unpin page in get_next after endkey check: " + eUnpin.getMessage());
            }
            leafPage=null; // Set leafPage to null as scan ends
            return null;
        }
      }

      return entry;
    }
    catch ( ScanIteratorException e) { // Catch specific exception first if needed
        throw e; // Rethrow if already the correct type
    }
    catch ( Exception e) {
         System.err.println("ERROR: BTFileScan.get_next(): UNEXPECTED EXCEPTION: " + e.getMessage()); // ADDED
         e.printStackTrace();
         // --- FIX: Use correct constructor ---
         throw new ScanIteratorException("Unexpected exception in BTFileScan.get_next: " + e.getMessage());
    }
  }


  /**
   * Delete currently-being-scanned(i.e., just scanned)
   * data entry.
   *@exception ScanDeleteException  delete error when scan
   */
  public void delete_current() 
    throws ScanDeleteException {

    KeyDataEntry entry;
    try{  
      if (leafPage == null) {
	System.out.println("No Record to delete!"); 
	throw new ScanDeleteException();
      }
      
      if( (deletedcurrent == true) || (didfirst==false) ) 
	return;    
      
      entry=leafPage.getCurrent(curRid);  
      SystemDefs.JavabaseBM.unpinPage( leafPage.getCurPage(), false);
      bfile.Delete(entry.key, ((LeafData)entry.data).getData());
      leafPage=bfile.findRunStart(entry.key, curRid);
      
      deletedcurrent = true;
      return;
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new ScanDeleteException();
    }  
  }
  
  /** max size of the key
   *@return the maxumum size of the key in BTFile
   */
  public int keysize() {
    return maxKeysize;
  }  
  
  
  
  /**
  * destructor.
  * unpin some pages if they are not unpinned already.
  * and do some clearing work.
  *@exception IOException  error from the lower layer
  *@exception bufmgr.InvalidFrameNumberException  error from the lower layer
  *@exception bufmgr.ReplacerException  error from the lower layer
  *@exception bufmgr.PageUnpinnedException  error from the lower layer
  *@exception bufmgr.HashEntryNotFoundException   error from the lower layer
  */
public void DestroyBTreeFileScan()
    throws IOException, bufmgr.InvalidFrameNumberException, bufmgr.ReplacerException,
           bufmgr.PageUnpinnedException, bufmgr.HashEntryNotFoundException
{
    System.out.println("DEBUG: BTFileScan.DestroyBTreeFileScan() called."); // Keep this
    if (leafPage != null) {
        System.out.println("DEBUG: BTFileScan.DestroyBTreeFileScan: Unpinning leaf page " + leafPage.getCurPage().pid + " (dirty=false)."); // MODIFIED Log
        try {
            // --- FIX: Unpin read-only page (dirty=false) ---
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false); // Use false for dirty flag
            System.out.println("DEBUG: BTFileScan.DestroyBTreeFileScan: ...unpinned successfully."); // Keep this
        } catch (Exception e) {
             System.err.println("ERROR: BTFileScan.DestroyBTreeFileScan: Exception during unpin: " + e.getMessage()); // Keep this
             // Rethrow relevant exceptions as per method signature
             if (e instanceof PageUnpinnedException) throw (PageUnpinnedException)e;
             if (e instanceof HashEntryNotFoundException) throw (HashEntryNotFoundException)e;
             if (e instanceof InvalidFrameNumberException) throw (InvalidFrameNumberException)e;
             if (e instanceof ReplacerException) throw (ReplacerException)e;
             if (e instanceof IOException) throw (IOException)e;
             // Wrap others if necessary
             throw new IOException("Wrapped exception during DestroyBTreeFileScan unpin: " + e.getMessage(), e);
        } finally {
             leafPage = null; // Ensure leafPage is nulled even if unpin fails
        }
    } else {
         System.out.println("DEBUG: BTFileScan.DestroyBTreeFileScan: leafPage was already null."); // Keep this
    }
     // leafPage=null; // Removed from here, moved to finally block above
     System.out.println("DEBUG: BTFileScan.DestroyBTreeFileScan() finished."); // Keep this
}




}





