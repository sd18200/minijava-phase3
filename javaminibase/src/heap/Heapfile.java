package heap;

import java.io.*;

import btree.UnpinPageException;
import diskmgr.*;
import bufmgr.*;
import global.*;

/**  This heapfile implementation is directory-based. We maintain a
 *  directory of info about the data pages (which are of type HFPage
 *  when loaded into memory).  The directory itself is also composed
 *  of HFPages, with each record being of type DataPageInfo
 *  as defined below.
 *
 *  The first directory page is a header page for the entire database
 *  (it is the one to which our filename is mapped by the DB).
 *  All directory pages are in a doubly-linked list of pages, each
 *  directory entry points to a single data page, which contains
 *  the actual records.
 *
 *  The heapfile data pages are implemented as slotted pages, with
 *  the slots at the front and the records in the back, both growing
 *  into the free space in the middle of the page.
 *
 *  We can store roughly pagesize/sizeof(DataPageInfo) records per
 *  directory page; for any given HeapFile insertion, it is likely
 *  that at least one of those referenced data pages will have
 *  enough free space to satisfy the request.
 */


/** DataPageInfo class : the type of records stored on a directory page.
*
* April 9, 1998
*/


interface  Filetype {
  int TEMP = 0;
  int ORDINARY = 1;
  
} // end of Filetype

public class Heapfile implements Filetype,  GlobalConst {
  
  
  PageId      _firstDirPageId;   // page number of header page
  int         _ftype;
  private     boolean     _file_deleted;
  private     String 	 _fileName;
  private static int tempfilecount = 0;
  
  
  public String get_fileName() {
    return _fileName;
}
  
  /* get a new datapage from the buffer manager and initialize dpinfo
     @param dpinfop the information in the new HFPage
  */
  private HFPage _newDatapage(DataPageInfo dpinfop)
    throws HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException
    {
      Page apage = new Page();
      PageId pageId = new PageId();
      pageId = newPage(apage, 1);
      
      if(pageId == null)
	throw new HFException(null, "can't new pae");
      
      // initialize internal values of the new page:
      
      HFPage hfpage = new HFPage();
      hfpage.init(pageId, apage);
      
      dpinfop.pageId.pid = pageId.pid;
      dpinfop.recct = 0;
      dpinfop.availspace = hfpage.available_space();
      
      return hfpage;
      
    } // end of _newDatapage
  
  /* Internal HeapFile function (used in getRecord and updateRecord):
     returns pinned directory page and pinned data page of the specified 
     user record(rid) and true if record is found.
     If the user record cannot be found, return false.
  */
  private boolean  _findDataPage( RID rid,
				  PageId dirPageId, HFPage dirpage,
				  PageId dataPageId, HFPage datapage,
				  RID rpDataPageRid) 
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   Exception
    {
      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
      
      HFPage currentDirPage = new HFPage();
      HFPage currentDataPage = new HFPage();
      RID currentDataPageRid = new RID();
      PageId nextDirPageId = new PageId();
      // datapageId is stored in dpinfo.pageId 
      
      
      pinPage(currentDirPageId, currentDirPage, false/*read disk*/);
      
      Tuple atuple = new Tuple();
      
      while (currentDirPageId.pid != INVALID_PAGE)
	{// Start While01
	  // ASSERTIONS:
	  //  currentDirPage, currentDirPageId valid and pinned and Locked.
	  
	  for( currentDataPageRid = currentDirPage.firstRecord();
	       currentDataPageRid != null;
	       currentDataPageRid = currentDirPage.nextRecord(currentDataPageRid))
	    {
	      try{
		atuple = currentDirPage.getRecord(currentDataPageRid);
	      }
	      catch (InvalidSlotNumberException e)// check error! return false(done) 
		{
		  return false;
		}
	      
	      DataPageInfo dpinfo = new DataPageInfo(atuple);
	      try{
		pinPage(dpinfo.pageId, currentDataPage, false/*Rddisk*/);
		
		
		//check error;need unpin currentDirPage
	      }catch (Exception e)
		{
		  unpinPage(currentDirPageId, false/*undirty*/);
		  dirpage = null;
		  datapage = null;
		  throw e;
		}
	      
	      
	      
	      // ASSERTIONS:
	      // - currentDataPage, currentDataPageRid, dpinfo valid
	      // - currentDataPage pinned
	      
	      if(dpinfo.pageId.pid==rid.pageNo.pid)
		{
		  atuple = currentDataPage.returnRecord(rid);
		  // found user's record on the current datapage which itself
		  // is indexed on the current dirpage.  Return both of these.
		  
		  dirpage.setpage(currentDirPage.getpage());
		  dirPageId.pid = currentDirPageId.pid;
		  
		  datapage.setpage(currentDataPage.getpage());
		  dataPageId.pid = dpinfo.pageId.pid;
		  
		  rpDataPageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
		  rpDataPageRid.slotNo = currentDataPageRid.slotNo;
		  return true;
		}
	      else
		{
		  // user record not found on this datapage; unpin it
		  // and try the next one
		  unpinPage(dpinfo.pageId, false /*undirty*/);
		  
		}
	      
	    }
	  
	  // if we would have found the correct datapage on the current
	  // directory page we would have already returned.
	  // therefore:
	  // read in next directory page:
	  
	  nextDirPageId = currentDirPage.getNextPage();
	  try{
	    unpinPage(currentDirPageId, false /*undirty*/);
	  }
	  catch(Exception e) {
	    throw new HFException (e, "heapfile,_find,unpinpage failed");
	  }
	  
	  currentDirPageId.pid = nextDirPageId.pid;
	  if(currentDirPageId.pid != INVALID_PAGE)
	    {
	      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
	      if(currentDirPage == null)
		throw new HFException(null, "pinPage return null page");  
	    }
	  
	  
	} // end of While01
      // checked all dir pages and all data pages; user record not found:(
      
      dirPageId.pid = dataPageId.pid = INVALID_PAGE;
      
      return false;   
      
      
    } // end of _findDatapage		     
  
  /** Initialize.  A null name produces a temporary heapfile which will be
   * deleted by the destructor.  If the name already denotes a file, the
   * file is opened; otherwise, a new empty file is created.
   *
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   */
  public Heapfile(String name) 
  throws HFException, 
       HFBufMgrException,
       HFDiskMgrException,
       IOException
{

  _file_deleted = true;
  _fileName = null;
  
  if(name == null) 
  {
    // If the name is NULL, allocate a temporary name
    _fileName = "tempHeapFile";
    String useId = new String("user.name");
    String userAccName;
    userAccName = System.getProperty(useId);
    _fileName = _fileName + userAccName;
    
    String filenum = Integer.toString(tempfilecount);
    _fileName = _fileName + filenum; 
    _ftype = TEMP;
    tempfilecount++;
  }
  else
  {
    _fileName = name;
    _ftype = ORDINARY;    
  }
  
  // The constructor gets run in two different cases.
  // In the first case, the file is new and the header page
  // must be initialized.  This case is detected via a failure
  // in the db->get_file_entry() call.  In the second case, the
  // file already exists and all that must be done is to fetch
  // the header page into the buffer pool
  
  // try to open the file
  
  Page apage = new Page();
  _firstDirPageId = null;
  
  long startTime = System.currentTimeMillis();
  
  if (_ftype == ORDINARY)
    _firstDirPageId = get_file_entry(_fileName);
  
  
  if(_firstDirPageId == null)
  {

    startTime = System.currentTimeMillis();
    
    _firstDirPageId = newPage(apage, 1);
    
    // check error
    if(_firstDirPageId == null)
      throw new HFException(null, "can't new page");
    
    startTime = System.currentTimeMillis();
    
    try {
      add_file_entry(_fileName, _firstDirPageId);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    HFPage firstDirPage = new HFPage();
    firstDirPage.init(_firstDirPageId, apage);
    PageId pageId = new PageId(INVALID_PAGE);
    
    firstDirPage.setNextPage(pageId);
    firstDirPage.setPrevPage(pageId);
    
    unpinPage(_firstDirPageId, true /*dirty*/ );
  }
  else {
  }
  
  _file_deleted = false;
} // end of constructor 
  
  /** Return number of records in file.
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   */
  public int getRecCnt() 
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   IOException
	   
    {
      int answer = 0;
      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
      
      PageId nextDirPageId = new PageId(0);
      
      HFPage currentDirPage = new HFPage();
      Page pageinbuffer = new Page();
      
      while(currentDirPageId.pid != INVALID_PAGE)
	{
	   pinPage(currentDirPageId, currentDirPage, false);
	   
	   RID rid = new RID();
	   Tuple atuple;
	   for (rid = currentDirPage.firstRecord();
	        rid != null;	// rid==NULL means no more record
	        rid = currentDirPage.nextRecord(rid))
	     {
	       atuple = currentDirPage.getRecord(rid);
	       DataPageInfo dpinfo = new DataPageInfo(atuple);
	       
	       answer += dpinfo.recct;
	     }
	   
	   // ASSERTIONS: no more record
           // - we have read all datapage records on
           //   the current directory page.
	   
	   nextDirPageId = currentDirPage.getNextPage();
	   unpinPage(currentDirPageId, false /*undirty*/);
	   currentDirPageId.pid = nextDirPageId.pid;
	}
      
      // ASSERTIONS:
      // - if error, exceptions
      // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
      // - if not yet end of heapfile: currentDirPageId valid
      
      
      return answer;
    } // end of getRecCnt
  
  /** Insert record into file, return its Rid.
   *
   * @param recPtr pointer of the record
   * @param recLen the length of the record
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception SpaceNotAvailableException no space left
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   *
   * @return the rid of the record
   */
  public RID insertRecord(byte[] recPtr) 
    throws InvalidSlotNumberException,  
	   InvalidTupleSizeException,
	   SpaceNotAvailableException,
	   HFException,
	   HFBufMgrException,
	   HFDiskMgrException,
	   IOException
    {
      int dpinfoLen = 0;	
      int recLen = recPtr.length;
      boolean found;
      RID currentDataPageRid = new RID();
      Page pageinbuffer = new Page();
      HFPage currentDirPage = new HFPage();
      HFPage currentDataPage = new HFPage();
      
      HFPage nextDirPage = new HFPage(); 
      PageId currentDirPageId = new PageId(_firstDirPageId.pid);
      PageId nextDirPageId = new PageId();  // OK
      
      pinPage(currentDirPageId, currentDirPage, false/*Rdisk*/);
      
      found = false;
      Tuple atuple;
      DataPageInfo dpinfo = new DataPageInfo();
      while (found == false)
	{ //Start While01
	  // look for suitable dpinfo-struct
	  for (currentDataPageRid = currentDirPage.firstRecord();
	       currentDataPageRid != null;
	       currentDataPageRid = 
		 currentDirPage.nextRecord(currentDataPageRid))
	    {
	      atuple = currentDirPage.getRecord(currentDataPageRid);
	      
	      dpinfo = new DataPageInfo(atuple);
	      
	      // need check the record length == DataPageInfo'slength
	      
	       if(recLen <= dpinfo.availspace)
		 {
		   found = true;
		   break;
		 }  
	    }
	  
	  // two cases:
	  // (1) found == true:
	  //     currentDirPage has a datapagerecord which can accomodate
	  //     the record which we have to insert
	  // (2) found == false:
	  //     there is no datapagerecord on the current directory page
	  //     whose corresponding datapage has enough space free
	  //     several subcases: see below
	  if(found == false)
	    { //Start IF01
	      // case (2)
	      
	      //System.out.println("no datapagerecord on the current directory is OK");
	      //System.out.println("dirpage availspace "+currentDirPage.available_space());
	      
	      // on the current directory page is no datapagerecord which has
	      // enough free space
	      //
	      // two cases:
	      //
	      // - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
	      //         if there is enough space on the current directory page
	      //         to accomodate a new datapagerecord (type DataPageInfo),
	      //         then insert a new DataPageInfo on the current directory
	      //         page
	      // - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
	      //         look at the next directory page, if necessary, create it.
	      
	      if(currentDirPage.available_space() >= dpinfo.size)
		{ 
		  //Start IF02
		  // case (2.1) : add a new data page record into the
		  //              current directory page
		  currentDataPage = _newDatapage(dpinfo); 
		  // currentDataPage is pinned! and dpinfo->pageId is also locked
		  // in the exclusive mode  
		  
		  // didn't check if currentDataPage==NULL, auto exception
		  
		  
		  // currentDataPage is pinned: insert its record
		  // calling a HFPage function
		  
		  
		  
		  atuple = dpinfo.convertToTuple();
		  
		  byte [] tmpData = atuple.getTupleByteArray();
		  currentDataPageRid = currentDirPage.insertRecord(tmpData);
		  
		  RID tmprid = currentDirPage.firstRecord();
		  
		  
		  // need catch error here!
		  if(currentDataPageRid == null)
		    throw new HFException(null, "no space to insert rec.");  
		  
		  // end the loop, because a new datapage with its record
		  // in the current directorypage was created and inserted into
		  // the heapfile; the new datapage has enough space for the
		  // record which the user wants to insert
		  
		  found = true;
		  
		} //end of IF02
	      else
		{  //Start else 02
		  // case (2.2)
		  nextDirPageId = currentDirPage.getNextPage();
		  // two sub-cases:
		  //
		  // (2.2.1) nextDirPageId != INVALID_PAGE:
		  //         get the next directory page from the buffer manager
		  //         and do another look
		  // (2.2.2) nextDirPageId == INVALID_PAGE:
		  //         append a new directory page at the end of the current
		  //         page and then do another loop
		    
		  if (nextDirPageId.pid != INVALID_PAGE) 
		    { //Start IF03
		      // case (2.2.1): there is another directory page:
		      unpinPage(currentDirPageId, false);
		      
		      currentDirPageId.pid = nextDirPageId.pid;
		      
		      pinPage(currentDirPageId,
						    currentDirPage, false);
		      
		      
		      
		      // now go back to the beginning of the outer while-loop and
		      // search on the current directory page for a suitable datapage
		    } //End of IF03
		  else
		    {  //Start Else03
		      // case (2.2): append a new directory page after currentDirPage
		      //             since it is the last directory page
		      nextDirPageId = newPage(pageinbuffer, 1);
		      // need check error!
		      if(nextDirPageId == null)
			throw new HFException(null, "can't new pae");
		      
		      // initialize new directory page
		      nextDirPage.init(nextDirPageId, pageinbuffer);
		      PageId temppid = new PageId(INVALID_PAGE);
		      nextDirPage.setNextPage(temppid);
		      nextDirPage.setPrevPage(currentDirPageId);
		      
		      // update current directory page and unpin it
		      // currentDirPage is already locked in the Exclusive mode
		      currentDirPage.setNextPage(nextDirPageId);
		      unpinPage(currentDirPageId, true/*dirty*/);
		      
		      currentDirPageId.pid = nextDirPageId.pid;
		      currentDirPage = new HFPage(nextDirPage);
		      
		      // remark that MINIBASE_BM->newPage already
		      // pinned the new directory page!
		      // Now back to the beginning of the while-loop, using the
		      // newly created directory page.
		      
		    } //End of else03
		} // End of else02
	      // ASSERTIONS:
	      // - if found == true: search will end and see assertions below
	      // - if found == false: currentDirPage, currentDirPageId
	      //   valid and pinned
	      
	    }//end IF01
	  else
	    { //Start else01
	      // found == true:
	      // we have found a datapage with enough space,
	      // but we have not yet pinned the datapage:
	      
	      // ASSERTIONS:
	      // - dpinfo valid
	      
	      // System.out.println("find the dirpagerecord on current page");
	      
	      pinPage(dpinfo.pageId, currentDataPage, false);
	      //currentDataPage.openHFpage(pageinbuffer);
	      
	      
	    }//End else01
	} //end of While01
      
      // ASSERTIONS:
      // - currentDirPageId, currentDirPage valid and pinned
      // - dpinfo.pageId, currentDataPageRid valid
      // - currentDataPage is pinned!
      
      if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
	throw new HFException(null, "invalid PageId");
      
      if (!(currentDataPage.available_space() >= recLen))
	throw new SpaceNotAvailableException(null, "no available space");
      
      if (currentDataPage == null)
	throw new HFException(null, "can't find Data page");
      
      
      RID rid;
      rid = currentDataPage.insertRecord(recPtr);
      
      dpinfo.recct++;
      dpinfo.availspace = currentDataPage.available_space();
      
      
      unpinPage(dpinfo.pageId, true /* = DIRTY */);
      
      // DataPage is now released
      atuple = currentDirPage.returnRecord(currentDataPageRid);
      DataPageInfo dpinfo_ondirpage = new DataPageInfo(atuple);
      
      
      dpinfo_ondirpage.availspace = dpinfo.availspace;
      dpinfo_ondirpage.recct = dpinfo.recct;
      dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
      dpinfo_ondirpage.flushToTuple();
      
      
      unpinPage(currentDirPageId, true /* = DIRTY */);
      
      
      return rid;
      
    }
  
  /** Delete record from file with given rid.
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception Exception other exception
   *
   * @return true record deleted  false:record not found
   */
  public boolean deleteRecord(RID rid)
    throws InvalidSlotNumberException,
           InvalidTupleSizeException,
           HFException,
           HFBufMgrException,
           HFDiskMgrException,
           Exception

    {
      boolean status = false; // Assume failure initially
      HFPage currentDirPage = new HFPage();
      PageId currentDirPageId = new PageId(INVALID_PAGE); // Initialize PageId to invalid
      HFPage currentDataPage = new HFPage();
      PageId currentDataPageId = new PageId(INVALID_PAGE); // Initialize PageId to invalid
      RID currentDataPageRid = new RID(); // rid of data page info on directory page
      boolean dataPageDirty = false; // Track if data page needs to be marked dirty
      boolean dirPageDirty = false;  // Track if directory page needs to be marked dirty
      boolean foundDataPage = false; // Flag to track if _findDataPage succeeded

      // Use try-finally to ensure pages are unpinned
      try {
          foundDataPage = _findDataPage(rid,
                                 currentDirPageId, currentDirPage,
                                 currentDataPageId, currentDataPage,
                                 currentDataPageRid);

          if (!foundDataPage) {
              // Record or page not found by _findDataPage.
              // _findDataPage should handle unpinning pages it pinned on failure.
              return false;
          }

          // ASSERTIONS after successful _findDataPage:
          // - currentDirPage, currentDirPageId valid and pinned (ASSUMING _findDataPage works correctly)
          // - currentDataPage, currentDataPageId valid and pinned (ASSUMING _findDataPage works correctly)

          // Get datapageinfo from the current directory page:
          Tuple atuple = currentDirPage.returnRecord(currentDataPageRid);
          DataPageInfo pdpinfo = new DataPageInfo(atuple);

          // Delete the record on the data page
          try {
              currentDataPage.deleteRecord(rid); // This might throw InvalidSlotNumberException
              dataPageDirty = true; // Mark data page dirty only if deleteRecord succeeds
          } catch (InvalidSlotNumberException e) {
              // If deleteRecord fails (e.g., invalid slot), we still need to unpin.
              // The finally block below will handle the unpinning.
              // Pages are not marked dirty in this case.
              status = false; // Ensure status remains false
              throw e; // Rethrow the specific exception
          }

          // If deleteRecord succeeded, update the directory page entry
          pdpinfo.recct--;
          pdpinfo.flushToTuple(); // Write changes back to the tuple buffer in memory
          dirPageDirty = true; // Mark directory page dirty as its content (pdpinfo) changed

          if (pdpinfo.recct >= 1)
          {
              // more records remain on datapage so it still hangs around.
              // we just need to modify its directory entry
              pdpinfo.availspace = currentDataPage.available_space();
              pdpinfo.flushToTuple(); // Write availspace changes back
              // Pages will be unpinned in the finally block with correct dirty status
          }
          else
          {
              // the record is already deleted:
              // we're removing the last record on datapage so free datapage
              // also, free the directory page if
              //   a) it's not the first directory page, and
              //   b) we've removed the last DataPageInfo record on it.

              // Free the empty datapage. It's already marked dirty=true if delete succeeded.
              // We need to unpin it first (without marking dirty again) before freeing.
              try {
                  unpinPage(currentDataPageId, false /* data page is going away, don't write */);
                  dataPageDirty = false; // Reset flag as it's handled by this unpin
              } catch (HFBufMgrException eUnpin) {
                  // This might happen if _findDataPage already unpinned it - log warning
                  System.err.println("Warning: Data page " + currentDataPageId.pid + " was already unpinned before freeing.");
                  dataPageDirty = false; // Ensure flag is false
              } catch (Exception eUnpin) {
                  System.err.println("Warning: Error unpinning data page " + currentDataPageId.pid + " before freeing: " + eUnpin);
                  // Continue to free page anyway if possible
              }
              freePage(currentDataPageId);
              currentDataPageId.pid = INVALID_PAGE; // Mark as invalid so finally block ignores it

              // delete corresponding DataPageInfo-entry on the directory page:
              // Note: currentDirPage is still pinned here
              currentDirPage.deleteRecord(currentDataPageRid);
              dirPageDirty = true; // Directory page was modified

              // now check whether the directory page is empty:
              RID firstRecOnDir = currentDirPage.firstRecord();
              PageId prevPageId = currentDirPage.getPrevPage();

              if ((firstRecOnDir == null) && (prevPageId.pid != INVALID_PAGE))
              {
                  // the directory-page is not the first directory page and it is empty:
                  // delete it

                  // point previous page around deleted page:
                  HFPage prevDirPage = new HFPage();
                  PageId tempPrevPageId = new PageId(prevPageId.pid); // Use a copy
                  pinPage(tempPrevPageId, prevDirPage, false);

                  PageId nextPageId = currentDirPage.getNextPage();
                  prevDirPage.setNextPage(nextPageId);
                  unpinPage(tempPrevPageId, true /* = DIRTY */);


                  // set prevPage-pointer of next Page
                  PageId tempNextPageId = new PageId(nextPageId.pid); // Use a copy
                  if(tempNextPageId.pid != INVALID_PAGE)
                  {
                      HFPage nextDirPage = new HFPage();
                      pinPage(tempNextPageId, nextDirPage, false);
                      nextDirPage.setPrevPage(tempPrevPageId); // Use the ID of the *previous* page
                      unpinPage(tempNextPageId, true /* = DIRTY */);
                  }

                  // delete empty directory page:
                  try {
                      // Unpin the directory page we are about to free
                      unpinPage(currentDirPageId, false /* dir page is going away, don't write */);
                      dirPageDirty = false; // Reset flag as it's handled by this unpin
                  } catch (HFBufMgrException eUnpin) {
                      // This might happen if _findDataPage already unpinned it - log warning
                      System.err.println("Warning: Directory page " + currentDirPageId.pid + " was already unpinned before freeing.");
                      dirPageDirty = false; // Ensure flag is false
                  } catch (Exception eUnpin) {
                      System.err.println("Warning: Error unpinning directory page " + currentDirPageId.pid + " before freeing: " + eUnpin);
                      // Continue to free page anyway if possible
                  }
                  freePage(currentDirPageId);
                  currentDirPageId.pid = INVALID_PAGE; // Mark as invalid so finally block ignores it
              }
              else
              {
                  // either (the directory page has at least one more datapagerecord
                  // entry) or (it is the first directory page):
                  // in both cases we do not delete it, but we have to unpin it
                  // (handled in finally block).
              }
          }

          status = true; // Mark overall operation as successful *only if no exception occurred*
          return true; // Deletion successful

      } finally {
          // Ensure pages pinned by _findDataPage (and not already handled above) are unpinned.
          // Check if IDs are valid before attempting to unpin.

          // Unpin Data Page if it's still valid and was found
          if (foundDataPage && currentDataPageId != null && currentDataPageId.pid != INVALID_PAGE) {
              try {
                  unpinPage(currentDataPageId, dataPageDirty);
              } catch (HFBufMgrException eUnpin) {
                  // Log specific warning for already unpinned page
                  System.err.println("Warning: Data page " + currentDataPageId.pid + " was already unpinned in deleteRecord finally block.");
              } catch (Exception eUnpin) {
                  // Log generic warning for other unpin errors
                  System.err.println("Warning: Error unpinning data page " + currentDataPageId.pid + " in deleteRecord finally: " + eUnpin);
              }
          }

          // Unpin Directory Page if it's still valid and was found
          if (foundDataPage && currentDirPageId != null && currentDirPageId.pid != INVALID_PAGE) {
              try {
                  unpinPage(currentDirPageId, dirPageDirty);
              } catch (HFBufMgrException eUnpin) {
                  // Log specific warning for already unpinned page
                  System.err.println("Warning: Directory page " + currentDirPageId.pid + " was already unpinned in deleteRecord finally block.");
              } catch (Exception eUnpin) {
                  // Log generic warning for other unpin errors
                  System.err.println("Warning: Error unpinning directory page " + currentDirPageId.pid + " in deleteRecord finally: " + eUnpin);
              }
          }
      }
    }
  
  
  /** Updates the specified record in the heapfile.
   * @param rid: the record which needs update
   * @param newtuple: the new content of the record
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidUpdateException invalid update on record
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception Exception other exception
   * @return ture:update success   false: can't find the record
   */
  public boolean updateRecord(RID rid, Tuple newtuple) 
    throws InvalidSlotNumberException, 
	   InvalidUpdateException, 
	   InvalidTupleSizeException,
	   HFException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   Exception
    {
      boolean status;
      HFPage dirPage = new HFPage();
      PageId currentDirPageId = new PageId();
      HFPage dataPage = new HFPage();
      PageId currentDataPageId = new PageId();
      RID currentDataPageRid = new RID();
      
      status = _findDataPage(rid,
			     currentDirPageId, dirPage, 
			     currentDataPageId, dataPage,
			     currentDataPageRid);
      
      if(status != true) return status;	// record not found
      Tuple atuple = new Tuple();
      atuple = dataPage.returnRecord(rid);
      
      // Assume update a record with a record whose length is equal to
      // the original record
      
      if(newtuple.getLength() != atuple.getLength())
	{
	  unpinPage(currentDataPageId, false /*undirty*/);
	  unpinPage(currentDirPageId, false /*undirty*/);
	  
	  throw new InvalidUpdateException(null, "invalid record update");
	  
	}

      // new copy of this record fits in old space;
      atuple.tupleCopy(newtuple);
      unpinPage(currentDataPageId, true /* = DIRTY */);
      
      unpinPage(currentDirPageId, false /*undirty*/);
      
      
      return true;
    }
  
  
  /** Read record from file, returning pointer and length.
   * @param rid Record ID
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception SpaceNotAvailableException no space left
   * @exception HFException heapfile exception
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception Exception other exception
   *
   * @return a Tuple. if Tuple==null, no more tuple
   */
  public  Tuple getRecord(RID rid) 
    throws InvalidSlotNumberException, 
	   InvalidTupleSizeException, 
	   HFException, 
	   HFDiskMgrException,
	   HFBufMgrException,
	   Exception
    {
      boolean status;
      HFPage dirPage = new HFPage();
      PageId currentDirPageId = new PageId();
      HFPage dataPage = new HFPage();
      PageId currentDataPageId = new PageId();
      RID currentDataPageRid = new RID();
      
      status = _findDataPage(rid,
			     currentDirPageId, dirPage, 
			     currentDataPageId, dataPage,
			     currentDataPageRid);
      
      if(status != true) return null; // record not found 
      
      Tuple atuple = new Tuple();
      atuple = dataPage.getRecord(rid);
      
      /*
       * getRecord has copied the contents of rid into recPtr and fixed up
       * recLen also.  We simply have to unpin dirpage and datapage which
       * were originally pinned by _findDataPage.
       */    
      
      unpinPage(currentDataPageId,false /*undirty*/);
      
      unpinPage(currentDirPageId,false /*undirty*/);
      
      
      return  atuple;  //(true?)OK, but the caller need check if atuple==NULL
      
    }
  
  
  /** Initiate a sequential scan.
   * @exception InvalidTupleSizeException Invalid tuple size
   * @exception IOException I/O errors
   *
   */
  public Scan openScan() 
    throws InvalidTupleSizeException,
	   IOException
    {
      Scan newscan = new Scan(this);
      return newscan;
    }
  
  
  /** Delete the file from the database.
   *
   * @exception InvalidSlotNumberException invalid slot number
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception FileAlreadyDeletedException file is deleted already
   * @exception HFBufMgrException exception thrown from bufmgr layer
   * @exception HFDiskMgrException exception thrown from diskmgr layer
   * @exception IOException I/O errors
   */
  public void deleteFile()
    throws InvalidSlotNumberException,
           FileAlreadyDeletedException,
           InvalidTupleSizeException,
           HFBufMgrException,
           HFDiskMgrException,
           IOException
    {
      if(_file_deleted )
        throw new FileAlreadyDeletedException(null, "file alread deleted");


      // Mark the deleted flag (even if it doesn't get all the way done).
      _file_deleted = true;

      // Deallocate all data pages
      PageId currentDirPageId = new PageId();
      currentDirPageId.pid = _firstDirPageId.pid;
      PageId nextDirPageId = new PageId();
      nextDirPageId.pid = 0;
      Page pageinbuffer = new Page();
      HFPage currentDirPage =  new HFPage();
      Tuple atuple;

      // Need to handle the case where _firstDirPageId might be invalid if file creation failed partially
      if (currentDirPageId == null || currentDirPageId.pid == INVALID_PAGE) {
          try {
              delete_file_entry( _fileName );
          } catch (Exception e) {
          }
          return; 
      }


      pinPage(currentDirPageId, currentDirPage, false);

      RID rid = new RID();
      while(currentDirPageId.pid != INVALID_PAGE)
        {
          for(rid = currentDirPage.firstRecord();
              rid != null;
              rid = currentDirPage.nextRecord(rid))
            {
              atuple = currentDirPage.getRecord(rid);
              DataPageInfo dpinfo = new DataPageInfo( atuple);
              freePage(dpinfo.pageId);

            }

          nextDirPageId = currentDirPage.getNextPage();
          freePage(currentDirPageId); 

          currentDirPageId.pid = nextDirPageId.pid;
          if (nextDirPageId.pid != INVALID_PAGE)
            {
              pinPage(currentDirPageId, currentDirPage, false);
            }
        }

      delete_file_entry( _fileName );
    }
  
  /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws HFBufMgrException {
    
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: pinPage() failed");
    }
    
  } 

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: unpinPage() failed");
    }

  } 

  private void freePage(PageId pageno)
    throws HFBufMgrException {

    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: freePage() failed");
    }

  } 

  private PageId newPage(Page page, int num)
    throws HFBufMgrException {

    PageId tmpId = new PageId();

    try {
      tmpId = SystemDefs.JavabaseBM.newPage(page,num);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"Heapfile.java: newPage() failed");
    }

    return tmpId;

  }

  private PageId get_file_entry(String filename)
    throws HFDiskMgrException {

    PageId tmpId = new PageId();

    try {
      tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
    }
    catch (Exception e) {
      throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
    }

    return tmpId;

  } 

  private void add_file_entry(String filename, PageId pageno)
    throws HFDiskMgrException {

    try {
      SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
    }
    catch (Exception e) {
      throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
    }

  } 

  private void delete_file_entry(String filename)
    throws HFDiskMgrException {

    try {
      SystemDefs.JavabaseDB.delete_file_entry(filename);
    }
    catch (Exception e) {
      throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
    }

  } 
  
}
