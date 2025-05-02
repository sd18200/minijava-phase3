/* File DB.java */

package diskmgr;

import java.io.*;
import bufmgr.*;
import global.*;

public class DB implements GlobalConst {

  
  private static final int bits_per_page = MAX_SPACE * 8;
  
  
  /** Open the database with the given name.
   *
   * @param name DB_name
   *
   * @exception IOException I/O errors
   * @exception FileIOException file I/O error
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void openDB( String fname)
    throws IOException, 
	   InvalidPageNumberException, 
	   FileIOException,
	   DiskMgrException {
    
    name = fname;

    File dbFile = new File(fname);
    if (!dbFile.exists()) {
        // Throw an exception that the caller (DBInterface) expects when the file is missing.
        // FileNotFoundException is a standard and appropriate choice.
        throw new FileNotFoundException("Database file not found: " + fname);
    }
    
    // Creaat a random access file
    fp = new RandomAccessFile(fname, "rw");
    
    PageId pageId = new PageId();
    Page apage = new Page();
    pageId.pid = 0;
    
    num_pages = 1;	//temporary num_page value for pinpage to work
    
    pinPage(pageId, apage, false /*read disk*/);
    
    
    DBFirstPage firstpg = new DBFirstPage();
    firstpg.openPage(apage);
    num_pages = firstpg.getNumDBPages();
    
    unpinPage(pageId, false /* undirty*/);
  }
  
  /** default constructor.
   */
  public DB() { }
  
  
  /** DB Constructors.
   * Create a database with the specified number of pages where the page
   * size is the default page size.
   *
   * @param name DB name
   * @param num_pages number of pages in DB
   *
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception DiskMgrException error caused by other layers
   */
  public void openDB( String fname, int num_pgs)
    throws IOException,
           InvalidPageNumberException,
           FileIOException,
           DiskMgrException {

    name = new String(fname);
    // Ensure at least 2 pages (header + space map)
    num_pages = (num_pgs > 2) ? num_pgs : 2;

    File DBfile = new File(name);
    // Log the absolute path it will try to use
    System.out.println("DEBUG: DB.openDB(create) - Target file path: " + DBfile.getAbsolutePath());

    // Delete the file if it already exists to ensure a fresh start
    // Note: This doesn't throw an error if the file doesn't exist.
    //DBfile.delete();

    // Create a random access file. "rw" mode creates the file if it doesn't exist.
    try {
        System.out.println("DEBUG: DB.openDB(create) - Attempting to create RandomAccessFile...");
        fp = new RandomAccessFile(fname, "rw");
        System.out.println("DEBUG: DB.openDB(create) - RandomAccessFile created successfully.");
    } catch (IOException e) {
        // Log failure details if RandomAccessFile creation fails (e.g., permissions)
        System.err.println("DEBUG: DB.openDB(create) - FAILED to create RandomAccessFile for: " + DBfile.getAbsolutePath());
        throw e; // Re-throw the original exception
    }

    // Make the file num_pages pages long, filled with zeroes.
    // Seek to the last byte of the last page and write a zero.
    fp.seek((long)num_pages*MINIBASE_PAGESIZE -1);
    fp.writeByte(0);

    // Initialize space map and directory pages.

    // Initialize the first DB page (Page 0 - Header/Directory)
    PageId pageId = new PageId();
    pageId.pid = 0;
    Page apage = new Page();
    // Pin the page, marking it as empty (true) since we are initializing it, not reading existing content.
    pinPage(pageId, apage, true /*emptyPage*/);

    // Treat the pinned page as the first page structure
    DBFirstPage firstpg = new DBFirstPage(apage);

    // Write the total number of pages into the header
    firstpg.setNumDBPages(num_pages);

    // Unpin the first page, marking it as dirty (true) so changes are written back.
    unpinPage(pageId, true /*dirty*/);

    // Calculate how many pages are needed for the space map.
    // The space map requires 1 bit per page of the database.
    // Reserve pages 0 (header) and 1 onwards for the space map.
    int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;

    // Mark the header page (0) and all space map pages (1 to num_map_pages) as allocated (bit = 1)
    // The run starts at page 0 and has a size of 1 (header) + num_map_pages.
    set_bits(new PageId(0), 1 + num_map_pages, 1); // Pass PageId(0) as start, and bit value 1

  }
  
  /** Close DB file.
   * @exception IOException I/O errors.
   */
  public void closeDB() throws IOException {
    fp.close();
  }
  
  
  /** Destroy the database, removing the file that stores it. 
   * @exception IOException I/O errors.
   */
  public void DBDestroy() 
    throws IOException {
    
    fp.close();
    File DBfile = new File(name);
    DBfile.delete();
  }
  
  /** Read the contents of the specified page into a Page object
   *
   * @param pageno pageId which will be read
   * @param apage page object which holds the contents of page
   *
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   */
  public  void read_page(PageId pageno, Page apage)
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException {

    if((pageno.pid < 0)||(pageno.pid >= num_pages))
      throw new InvalidPageNumberException(null, "BAD_PAGE_NUMBER");
    
    // Seek to the correct page
    fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));
    
    // Read the appropriate number of bytes.
    byte [] buffer = apage.getpage();  //new byte[MINIBASE_PAGESIZE];
    try{
      fp.read(buffer);
    }
    catch (IOException e) {
      throw new FileIOException(e, "DB file I/O error");
    }
    
  }
  
  /** Write the contents in a page object to the specified page.
   *
   * @param pageno pageId will be wrote to disk
   * @param apage the page object will be wrote to disk
   *
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   */
  public void write_page(PageId pageno, Page apage)
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException {

    if((pageno.pid < 0)||(pageno.pid >= num_pages))
      throw new InvalidPageNumberException(null, "INVALID_PAGE_NUMBER");
    
    // Seek to the correct page
    fp.seek((long)(pageno.pid *MINIBASE_PAGESIZE));
    
    // Write the appropriate number of bytes.
    try{
      fp.write(apage.getpage());
    }
    catch (IOException e) {
      throw new FileIOException(e, "DB file I/O error");
    }
    
  }
  
  /** Allocate a set of pages where the run size is taken to be 1 by default.
   *  Gives back the page number of the first page of the allocated run.
   *  with default run_size =1
   *
   * @param start_page_num page number to start with 
   *
   * @exception OutOfSpaceException database is full
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException DB file I/O errors
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void allocate_page(PageId start_page_num)
    throws OutOfSpaceException, 
	   InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   FileIOException, 
	   DiskMgrException,
           IOException {
    allocate_page(start_page_num, 1);
  }
  
  /** user specified run_size
   *
   * @param start_page_num the starting page id of the run of pages
   * @param run_size the number of page need allocated
   *
   * @exception OutOfSpaceException No space left
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void allocate_page(PageId start_page_num, int runsize)
    throws OutOfSpaceException, 
	   InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   FileIOException, 
	   DiskMgrException,
           IOException {

    if(runsize < 0) throw new InvalidRunSizeException(null, "Negative run_size");
    
    int run_size = runsize;
    int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
    int current_run_start = 0; 
    int current_run_length = 0;
    
    
    // This loop goes over each page in the space map.
    PageId pgid = new PageId();
    byte [] pagebuf;
    int byteptr;
    
    for(int i=0; i< num_map_pages; ++i) {// start forloop01
	
      pgid.pid = 1 + i;
      // Pin the space-map page.
      
      Page apage = new Page();
      pinPage(pgid, apage, false /*read disk*/);
      
      pagebuf = apage.getpage();
      byteptr = 0;
      
      // get the num of bits on current page
      int num_bits_this_page = num_pages - i*bits_per_page;
      if(num_bits_this_page > bits_per_page)
	num_bits_this_page = bits_per_page;
      
      // Walk the page looking for a sequence of 0 bits of the appropriate
      // length.  The outer loop steps through the page's bytes, the inner
      // one steps through each byte's bits.
      
      for(; num_bits_this_page>0 
	    && current_run_length < run_size; ++byteptr) {// start forloop02
	  
	
	Integer intmask = new Integer(1);
	Byte mask = new Byte(intmask.byteValue());
	byte tmpmask = mask.byteValue();
	
	while (mask.intValue()!=0 && (num_bits_this_page>0)
	       &&(current_run_length < run_size))
	  
	  {	      
	    if( (pagebuf[byteptr] & tmpmask ) != 0)
	      {
		current_run_start += current_run_length + 1;
		current_run_length = 0;
	      }
	    else ++current_run_length;
	    
	    
	    tmpmask <<=1;
	    mask = new Byte(tmpmask);
	    --num_bits_this_page;
	  }
	
	
      }//end of forloop02
      // Unpin the space-map page.
      
      unpinPage(pgid, false /*undirty*/);
      
    }// end of forloop01
    
    if(current_run_length >= run_size)
      {
	start_page_num.pid = current_run_start;
	set_bits(start_page_num, run_size, 1);
	
	return;
      }
    
    throw new OutOfSpaceException(null, "No space left");
  }
  
  /** Deallocate a set of pages starting at the specified page number and
   * a run size can be specified.
   *
   * @param start_page_num the start pageId to be deallocate
   * @param run_size the number of pages to be deallocated
   * 
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void deallocate_page(PageId start_page_num, int run_size)
    throws InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   IOException, 
	   FileIOException,
	   DiskMgrException {

    if(run_size < 0) throw new InvalidRunSizeException(null, "Negative run_size");
    
    set_bits(start_page_num, run_size, 0);
  }
  
  /** Deallocate a set of pages starting at the specified page number
   *  with run size = 1
   *
   * @param start_page_num the start pageId to be deallocate
   * @param run_size the number of pages to be deallocated
   *
   * @exception InvalidRunSizeException invalid run size 
   * @exception InvalidPageNumberException invalid page number
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   * 
   */
  public void deallocate_page(PageId start_page_num)
    throws InvalidRunSizeException, 
	   InvalidPageNumberException, 
	   IOException, 
	   FileIOException,
	   DiskMgrException {

    set_bits(start_page_num, 1, 0);
  }
  
  /** Adds a file entry to the header page(s).
   *
   * @param fname file entry name
   * @param start_page_num the start page number of the file entry
   *
   * @exception FileNameTooLongException invalid file name (too long)
   * @exception InvalidPageNumberException invalid page number
   * @exception InvalidRunSizeException invalid DB run size
   * @exception DuplicateEntryException entry for DB is not unique
   * @exception OutOfSpaceException database is full
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception DiskMgrException error caused by other layers
   */
  public void add_file_entry(String fname, PageId start_page_num)
    throws FileNameTooLongException,
           InvalidPageNumberException,
           InvalidRunSizeException,
           DuplicateEntryException,
           OutOfSpaceException,
           FileIOException,
           IOException,
           DiskMgrException {

    // Existing character length check (keep it)
    if(fname.length() >= MAX_NAME)
      throw new FileNameTooLongException(null, "DB filename character length too long");

    // *** ADDED: Check UTF-8 encoded byte length ***
    int encodedSize;
    try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(fname); // Simulate the encoding to get byte length
        dos.close(); // Close stream
        encodedSize = baos.size(); // Get the total size including the 2-byte length prefix
    } catch (IOException e) {
        // This shouldn't realistically happen with ByteArrayOutputStream
        throw new DiskMgrException(e, "Error checking filename byte length");
    }

    // Check if the encoded size fits in the allocated space for the string part
    // The allocated space is MAX_NAME + 2 bytes.
    if (encodedSize > MAX_NAME + 2) {
         throw new FileNameTooLongException(null, "DB filename encoded byte length (" + encodedSize + ") exceeds allocated space (" + (MAX_NAME + 2) + " bytes)");
    }
    // *** END ADDED CHECK ***


    if((start_page_num.pid < 0)||(start_page_num.pid >= num_pages))
      throw new InvalidPageNumberException(null, " DB bad page number");

    // Does the file already exist? Check before iterating to find a slot.
    if( get_file_entry(fname) != null)
      throw new DuplicateEntryException(null, "DB fileentry already exists");

    // --- Find a free slot ---
    Page apage = new Page();
    boolean found = false;
    int free_slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0); // Start search from page 0
    DBHeaderPage dp = null; // Initialize dp

    do
      {// Start DO01: Loop through directory pages
        hpid.pid = nexthpid.pid;

        // Pin the current directory page
        pinPage(hpid, apage, false /*read disk*/);

        // Instantiate the correct Page type (First vs Directory)
        if(hpid.pid==0)
          {
            dp = new DBFirstPage();
            ((DBFirstPage) dp).openPage(apage);
          }
        else
          {
            dp = new DBDirectoryPage();
            ((DBDirectoryPage) dp).openPage(apage);
          }

        // Get the next directory page ID for the loop condition
        nexthpid = dp.getNextPage();
        int entry = 0;
        int numEntriesOnPage = dp.getNumOfEntries();

        // *** MODIFIED LOOP TO FIND FREE SLOT ***
        PageId tempPid = new PageId(); // Temporary PageId to check slot status
        while (entry < numEntriesOnPage) {
            // Directly read only the PageID part of the slot to check if it's free
            // Calculation uses constants from DBHeaderPage
            int position = DBHeaderPage.START_FILE_ENTRIES + entry * DBHeaderPage.SIZE_OF_FILE_ENTRY;
            try {
                // Access dp.data which is protected (accessible within package diskmgr)
                tempPid.pid = Convert.getIntValue(position, dp.data);
            } catch (IOException e) {
                // Handle potential IOException from Convert.getIntValue
                unpinPage(hpid, false); // Unpin before throwing
                throw new DiskMgrException(e, "DB.java: Error reading PageID in free slot search");
            }

            if (tempPid.pid == INVALID_PAGE) {
                // Found a free slot
                break; // Exit the inner while loop
            }
            entry++; // Move to the next entry
        }
        // *** END MODIFIED LOOP ***


        if(entry < numEntriesOnPage) // Check if we broke the inner loop early (found free slot)
          {
            free_slot = entry;
            found = true;
            // Keep the page pinned (hpid), we will write the entry to it later.
          }
        else // No free slot found on this page
          {
            // Unpin if we are going to check the next directory page
            if (nexthpid.pid != INVALID_PAGE) {
                 unpinPage(hpid, false /* undirty*/);
            }
            // If nexthpid.pid == INVALID_PAGE, it means this was the *last* directory page
            // and it was full. Keep it pinned (hpid) because we need to update its
            // nextPage pointer after allocating a new one.
          }

      // Continue looping if there's a next page AND we haven't found a slot yet
      } while((nexthpid.pid != INVALID_PAGE) && (!found)); // End of DO01

    // --- Allocate a new directory page if needed ---
    if(!found)
      {
        // At this point, hpid is the *last* directory page, and dp points to it (pinned).
        try{
            // Allocate a new page, nexthpid will contain its ID
            allocate_page(nexthpid);
        }
        catch(Exception e){
            // If allocation fails, unpin the last directory page we were holding
            unpinPage(hpid, false /* undirty*/);
            System.err.println("DEBUG: DB.add_file_entry - Failed to allocate new directory page:");
            e.printStackTrace();
            throw new DiskMgrException(e, "DB.java: Failed to allocate new directory page");
        }

        // Set the next-page pointer on the *previous* directory page (dp).
        dp.setNextPage(nexthpid);
        // Unpin the *previous* directory page, marking it dirty.
        unpinPage(hpid, true /* dirty*/);

        // Update hpid to the ID of the *newly allocated* directory page.
        hpid.pid = nexthpid.pid;

        // Pin the *new* directory page, initializing its content (emptyPage=true).
        pinPage(hpid, apage, true/*emptyPage=true*/);
        // Initialize the new page structure using the DBDirectoryPage constructor.
        // This constructor calls the DBHeaderPage constructor which initializes slots correctly.
        dp = new DBDirectoryPage(apage);

        // The first slot on the new page is the free one.
        free_slot = 0;
        found = true; // Mark as found since we created a new page with a slot.
        // The new page (hpid) remains pinned, and dp points to it.
      }

    // --- Write the file entry ---
    // At this point:
    // - 'hpid' is the ID of the directory page with the free slot.
    // - 'apage' holds the pinned page data for hpid.
    // - 'dp' points to the DBHeaderPage object for hpid.
    // - 'free_slot' is the index of the free slot on that page.

    dp.setFileEntry(start_page_num, fname, free_slot);

    // Unpin the directory page where the entry was added, marking it dirty.
    unpinPage(hpid, true /* dirty*/);

  }
  
  /** Delete the entry corresponding to a file from the header page(s).
   *
   * @param fname file entry name
   *
   * @exception FileEntryNotFoundException file does not exist
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void delete_file_entry(String fname)
    throws FileEntryNotFoundException, 
	   IOException,
	   FileIOException,
	   InvalidPageNumberException, 
	   DiskMgrException {
    
    Page apage = new Page();
    boolean found = false;
    int slot = 0;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    PageId tmppid = new PageId();
    DBHeaderPage dp;
    
    do
      { // startDO01
        hpid.pid = nexthpid.pid;
	
	// Pin the header page.
	pinPage(hpid, apage, false/*read disk*/);

	// This complication is because the first page has a different
        // structure from that of subsequent pages.
	if(hpid.pid==0)
	  {
	    dp = new DBFirstPage();
	    ((DBFirstPage)dp).openPage(apage);
	  }
	else
	  {
	    dp = new DBDirectoryPage();
	    ((DBDirectoryPage) dp).openPage(apage);
	  }
	nexthpid = dp.getNextPage();
	
	int entry = 0;
	
	String tmpname;
	while(entry < dp.getNumOfEntries())
	  {
	    tmpname = dp.getFileEntry(tmppid, entry);
	    
	    if((tmppid.pid != INVALID_PAGE)&&
	       (tmpname.compareTo(fname) == 0)) break; 
	    entry ++;
	  }
	
        if(entry < dp.getNumOfEntries())
	  {
	    slot = entry;
	    found = true;
	  }
	else
	  {
	    unpinPage(hpid, false /*undirty*/);
	  }
	
      } while((nexthpid.pid != INVALID_PAGE) && (!found)); // EndDO01
    
    if(!found)  // Entry not found - nothing deleted
      throw new FileEntryNotFoundException(null, "DB file not found");
    
    // Have to delete record at hpnum:slot
    tmppid.pid = INVALID_PAGE;
    dp.setFileEntry(tmppid, "\0", slot);
    
    unpinPage(hpid, true /*dirty*/);
    
  }
  
  /** Get the entry corresponding to the given file.
   *
   * @param name file entry name
   *
   * @exception IOException I/O errors
   * @exception FileIOException file I/O error
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public PageId get_file_entry(String name)
    throws IOException,
       FileIOException,
       InvalidPageNumberException,
       DiskMgrException {

    System.out.println("DEBUG: DB.get_file_entry - Entered for name: " + name); // Keep: Method entry
    Page apage = new Page();
    boolean found = false;
    PageId hpid = new PageId();
    PageId nexthpid = new PageId(0);
    DBHeaderPage dp = null;
    PageId startpid = null;

    try {
        do
        {
            hpid.pid = nexthpid.pid;
            // System.out.println("DEBUG: DB.get_file_entry - Loop start, hpid: " + hpid.pid); // Commented out

            // System.out.println("DEBUG: DB.get_file_entry - About to pin page: " + hpid.pid); // Commented out
            pinPage(hpid, apage, false /*read disk*/); // Read from disk if not in buffer
            // System.out.println("DEBUG: DB.get_file_entry - Pinned page: " + hpid.pid); // Commented out

            if(hpid.pid==0)
            {
                dp = new DBFirstPage();
                ((DBFirstPage) dp).openPage(apage);
            }
            else
            {
                dp = new DBDirectoryPage();
                ((DBDirectoryPage) dp).openPage(apage);
            }
            // System.out.println("DEBUG: DB.get_file_entry - About to get next page ID from page: " + hpid.pid); // Commented out
            nexthpid = dp.getNextPage();
            // System.out.println("DEBUG: DB.get_file_entry - Got next page ID: " + nexthpid.pid); // Commented out

            int entry = 0;
            PageId tmppid = new PageId();
            String tmpname = null; // Initialize tmpname
            int numEntries = dp.getNumOfEntries();
            // System.out.println("DEBUG: DB.get_file_entry - Num entries on page " + hpid.pid + ": " + numEntries); // Commented out

            while(entry < numEntries)
            {
                // System.out.println("DEBUG: DB.get_file_entry - Checking entry: " + entry + " on page " + hpid.pid); // Commented out

                // 1. Read ONLY the PageID first to check if the slot is used.
                int position = DBHeaderPage.START_FILE_ENTRIES + entry * DBHeaderPage.SIZE_OF_FILE_ENTRY;
                try {
                    tmppid.pid = Convert.getIntValue(position, dp.data);
                } catch (IOException e) {
                    // Handle potential IOException from Convert.getIntValue
                    System.err.println("DEBUG: DB.get_file_entry - *** IOException reading PageID for entry " + entry + " on page " + hpid.pid + ": " + e); // Keep Error
                    unpinPage(hpid, false); // Unpin before throwing
                    throw new DiskMgrException(e, "DB.java: Error reading PageID in get_file_entry search");
                }

                // 2. If the PageID is NOT INVALID_PAGE, then the slot is used.
                //    Proceed to read the full entry (including the string) and compare.
                if (tmppid.pid != INVALID_PAGE) {
                    // System.out.println("DEBUG: DB.get_file_entry - Slot " + entry + " is used (pid=" + tmppid.pid + "). Reading full entry."); // Keep: Shows logic path
                    try {
                        // --- Optional: Debug Bytes (Commented out) ---
                        // int strOffset = position + 4;
                        // int strMaxLength = DBHeaderPage.MAX_NAME + 2;
                        // System.out.print("DEBUG: Bytes at offset " + strOffset + " for entry " + entry + ": ");
                        // for (int i = 0; i < Math.min(10, strMaxLength); i++) {
                        //     if (strOffset + i < dp.data.length) { System.out.printf("%02X ", dp.data[strOffset + i]); }
                        //     else { System.out.print("OOB "); }
                        // }
                        // System.out.println();
                        // --- End Debug Bytes ---

                        // Now read the full entry (PID is re-read here, but that's okay)
                        tmpname = dp.getFileEntry(tmppid, entry); // Reads PID and String
                        // System.out.println("DEBUG: DB.get_file_entry - Read entry " + entry + ": pid=" + tmppid.pid + ", name=" + (tmpname != null ? "'" + tmpname + "'" : "null")); // Commented out

                        // Compare the name if successfully read
                        if((tmpname != null) && (tmpname.compareTo(name) == 0)) {
                             System.out.println("DEBUG: DB.get_file_entry - Found entry '" + name + "' at slot " + entry + " on page " + hpid.pid); // Keep: Important event
                             startpid = new PageId(tmppid.pid); // Assign the found pid
                             found = true;
                        }

                    } catch (UTFDataFormatException utfEx) {
                        System.err.println("DEBUG: DB.get_file_entry - *** UTFDataFormatException for USED entry " + entry + " on page " + hpid.pid + " ***"); // Keep Error
                        throw utfEx; // Re-throw the original exception
                    } catch (IOException ioEx) {
                         System.err.println("DEBUG: DB.get_file_entry - *** IOException during getFileEntry call for USED entry " + entry + " on page " + hpid.pid + ": " + ioEx); // Keep Error
                         throw ioEx; // Re-throw the original exception
                    }
                } else {
                    // If tmppid.pid == INVALID_PAGE, the slot is unused. Skip reading the string.
                    // System.out.println("DEBUG: DB.get_file_entry - Slot " + entry + " is unused (pid=INVALID_PAGE). Skipping."); // Keep: Shows fix is working
                }

                // Exit loop if found
                if (found) {
                    break;
                }
                // Move to the next entry
                entry++;

            } // End inner while

            // System.out.println("DEBUG: DB.get_file_entry - About to unpin page: " + hpid.pid); // Commented out
            unpinPage(hpid, false /*undirty*/);
            // System.out.println("DEBUG: DB.get_file_entry - Unpinned page: " + hpid.pid); // Commented out

            if (found) {
                // System.out.println("DEBUG: DB.get_file_entry - Breaking outer loop as entry found."); // Commented out
                break; // Exit outer loop
            }
             // System.out.println("DEBUG: DB.get_file_entry - Loop end, continuing to next page: " + nexthpid.pid); // Commented out

        } while(nexthpid.pid != INVALID_PAGE);

        if (found) {
             System.out.println("DEBUG: DB.get_file_entry - Returning found pid: " + startpid.pid); // Keep: Method exit
        } else {
             System.out.println("DEBUG: DB.get_file_entry - Returning null (entry not found)"); // Keep: Method exit
        }
        return startpid;

    } catch (Exception e) { // Catch unexpected exceptions outside the inner try-catch
        System.err.println("DEBUG: DB.get_file_entry - *** UNEXPECTED EXCEPTION *** (outer catch) for page " + hpid.pid + ": " + e); // Keep Error
        e.printStackTrace();
        if (e instanceof IOException) throw (IOException)e;
        if (e instanceof FileIOException) throw (FileIOException)e;
        if (e instanceof InvalidPageNumberException) throw (InvalidPageNumberException)e;
        if (e instanceof DiskMgrException) throw (DiskMgrException)e;
        throw new DiskMgrException(e, "DB.java: get_file_entry() failed unexpectedly");
    }
}
  
  /** Functions to return some characteristics of the database.
   */
  public String db_name(){return name;}
  public int db_num_pages(){return num_pages;}
  public int db_page_size(){return MINIBASE_PAGESIZE;}
  
  /** Print out the space map of the database.
   * The space map is a bitmap showing which
   * pages of the db are currently allocated.
   *
   * @exception FileIOException file I/O error
   * @exception IOException I/O errors
   * @exception InvalidPageNumberException invalid page number
   * @exception DiskMgrException error caused by other layers
   */
  public void dump_space_map()
    throws DiskMgrException,
	   IOException,
	   FileIOException, 
	   InvalidPageNumberException 
	   
    {
      
      System.out.println ("********  IN DUMP");
      int num_map_pages = (num_pages + bits_per_page -1)/bits_per_page;
      int bit_number = 0;
      
      // This loop goes over each page in the space map.
      PageId pgid = new PageId();
      System.out.println ("num_map_pages = " + num_map_pages);
      System.out.println ("num_pages = " + num_pages);
      for(int i=0; i< num_map_pages; i++)
	{//start forloop01
	  
	  pgid.pid = 1 + i;   //space map starts at page1
	  // Pin the space-map page.
	  Page apage = new Page();
	  pinPage(pgid, apage, false/*read disk*/);
	  
	  // How many bits should we examine on this page?
	  int num_bits_this_page = num_pages - i*bits_per_page;
	  System.out.println ("num_bits_this_page = " + num_bits_this_page);
	  System.out.println ("num_pages = " + num_pages);
	  if ( num_bits_this_page > bits_per_page )
	    num_bits_this_page = bits_per_page;
	  
	  // Walk the page looking for a sequence of 0 bits of the appropriate
	  // length.  The outer loop steps through the page's bytes, the inner
	  // one steps through each byte's bits.
	  
	  int pgptr = 0;
	  byte [] pagebuf = apage.getpage();
	  int mask;
	  for ( ; num_bits_this_page > 0; pgptr ++)
	    {// start forloop02
	      
	      for(mask=1;
		  mask < 256 && num_bits_this_page > 0;
		  mask=(mask<<1), --num_bits_this_page, ++bit_number )
		{//start forloop03
		  
		  int bit = pagebuf[pgptr] & mask;
		  if((bit_number%10) == 0)
		    if((bit_number%50) == 0)
		      {
			if(bit_number>0) System.out.println("\n");
			System.out.print("\t" + bit_number +": ");
		      }
		    else System.out.print(' ');
		  
		  if(bit != 0) System.out.print("1");
		  else System.out.print("0");
		  
		}//end of forloop03
	      
	    }//end of forloop02
	  
	  unpinPage(pgid, false /*undirty*/);
	  
	}//end of forloop01
      
      System.out.println();
      
      
    }
  
  private RandomAccessFile fp;
  private int num_pages;
  private String name;
  
  
  /** Set runsize bits starting from start to value specified
   */
  private void set_bits( PageId start_page, int run_size, int bit )
    throws InvalidPageNumberException, 
	   FileIOException, 
	   IOException, 
	   DiskMgrException {

    if((start_page.pid<0) || (start_page.pid+run_size > num_pages))
      throw new InvalidPageNumberException(null, "Bad page number");
    
    // Locate the run within the space map.
    int first_map_page = start_page.pid/bits_per_page + 1;
    int last_map_page = (start_page.pid+run_size-1)/bits_per_page +1;
    int first_bit_no = start_page.pid % bits_per_page;
    
    // The outer loop goes over all space-map pages we need to touch.
    
    for(PageId pgid = new PageId(first_map_page);
	pgid.pid <= last_map_page;
	pgid.pid = pgid.pid+1, first_bit_no = 0)
      {//Start forloop01
	
        // Pin the space-map page.
	Page pg = new Page();
	
	
	pinPage(pgid, pg, false/*no diskIO*/);
	
	
	byte [] pgbuf = pg.getpage();
	
	// Locate the piece of the run that fits on this page.
	int first_byte_no = first_bit_no/8;
	int first_bit_offset = first_bit_no%8;
	int last_bit_no = first_bit_no + run_size -1;
	
	if(last_bit_no >= bits_per_page )
	  last_bit_no = bits_per_page - 1;
	
        int last_byte_no = last_bit_no / 8;
        
	// This loop actually flips the bits on the current page.
	int cur_posi = first_byte_no;
	for(;cur_posi <= last_byte_no; ++cur_posi, first_bit_offset=0)
	  {//start forloop02
	    
	    int max_bits_this_byte = 8 - first_bit_offset;
	    int num_bits_this_byte = (run_size > max_bits_this_byte?
				      max_bits_this_byte : run_size);
	    
            int imask =1;
	    int temp;
	    imask = ((imask << num_bits_this_byte) -1)<<first_bit_offset;
	    Integer intmask = new Integer(imask);
	    Byte mask = new Byte(intmask.byteValue());
	    byte bytemask = mask.byteValue();
	    
	    if(bit==1)
	      {
	        temp = (pgbuf[cur_posi] | bytemask);
	        intmask = new Integer(temp);
		pgbuf[cur_posi] = intmask.byteValue();
	      }
	    else
	      {
		
		temp = pgbuf[cur_posi] & (255^bytemask);
	        intmask = new Integer(temp);
		pgbuf[cur_posi] = intmask.byteValue();
	      }
	    run_size -= num_bits_this_byte;
	    
	  }//end of forloop02
	
	// Unpin the space-map page.
	
	unpinPage(pgid, true /*dirty*/);
	
      }//end of forloop01
    
  }

  /**
   * short cut to access the pinPage function in bufmgr package.
   * @see bufmgr.pinPage
   */
  private void pinPage(PageId pageno, Page page, boolean emptyPage)
    throws DiskMgrException {

    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new DiskMgrException(e,"DB.java: pinPage() failed");
    }

  } // end of pinPage

  /**
   * short cut to access the unpinPage function in bufmgr package.
   * @see bufmgr.unpinPage
   */
  private void unpinPage(PageId pageno, boolean dirty)
    throws DiskMgrException {

    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty); 
    }
    catch (Exception e) {
      throw new DiskMgrException(e,"DB.java: unpinPage() failed");
    }

  } // end of unpinPage
  
  
}//end of DB class

/**
 * interface of PageUsedBytes
 */
interface PageUsedBytes
{
  int DIR_PAGE_USED_BYTES = 8 + 8;
  int FIRST_PAGE_USED_BYTES = DIR_PAGE_USED_BYTES + 4;
}

/** Super class of the directory page and first page
 */
class DBHeaderPage implements PageUsedBytes, GlobalConst { 

  protected static final int NEXT_PAGE = 0;
  protected static final int NUM_OF_ENTRIES = 4;
  protected static final int START_FILE_ENTRIES = 8;
  protected static final int SIZE_OF_FILE_ENTRY = 4 + MAX_NAME + 2;
  
  protected byte [] data;
  
  /**
   * Default constructor
   */
  public DBHeaderPage ()
    {  }
  
  /**
   * Constrctor of class DBHeaderPage
   * @param page a page of Page object
   * @param pageusedbytes number of bytes used on the page
   * @exception IOException
   */   
  public DBHeaderPage(Page page, int pageusedbytes)
    throws IOException
    {
      data = page.getpage();
      PageId pageno = new PageId();
      pageno.pid = INVALID_PAGE;
      setNextPage(pageno);
      
      PageId temppid = getNextPage();
      
      int num_entries  = (MAX_SPACE - pageusedbytes) /SIZE_OF_FILE_ENTRY; 
      setNumOfEntries(num_entries);
      
      for ( int index=0; index < num_entries; ++index )
        initFileEntry(INVALID_PAGE,  index);
    }
  
  /**
   * set the next page number
   * @param pageno next page ID 
   * @exception IOException I/O errors
   */
  public void setNextPage(PageId pageno)
    throws IOException
    {
      Convert.setIntValue(pageno.pid, NEXT_PAGE, data);
    }
  
  /**
   * return the next page number
   * @return next page ID
   * @exception IOException I/O errors
   */
  public PageId getNextPage()
    throws IOException
    {
      PageId nextPage = new PageId();
      nextPage.pid= Convert.getIntValue(NEXT_PAGE, data);
      return nextPage;
    }
  
  /**
   * set number of entries on this page
   * @param numEntries the number of entries
   * @exception IOException I/O errors
   */
  
  protected void setNumOfEntries(int numEntries) 
    throws IOException	
    { 
      Convert.setIntValue (numEntries, NUM_OF_ENTRIES, data);
    }
  
  /**
   * return the number of file entries on the page
   * @return number of entries
   * @exception IOException I/O errors
   */  
  public int getNumOfEntries()
    throws IOException
    {
      return Convert.getIntValue(NUM_OF_ENTRIES, data);
    }
  
  /**
   * initialize file entries as empty
   * @param empty invalid page number (=-1)
   * @param entryno file entry number
   * @exception IOException I/O errors
   */
  private void initFileEntry(int empty, int entryNo)
    throws IOException {
    int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
    Convert.setIntValue (empty, position, data);
  } 
  
  /**
   * set file entry
   * @param pageno page ID
   * @param fname the file name
   * @param entryno file entry number
   * @exception IOException I/O errors
   */  
  public  void setFileEntry(PageId pageNo, String fname, int entryNo)
    throws IOException {

    int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
    Convert.setIntValue (pageNo.pid, position, data);
    Convert.setStrValue (fname, position +4, data);	
  }
  
  /**
   * return file entry info
   * @param pageno page Id
   * @param entryNo the file entry number
   * @return file name
   * @exception IOException I/O errors
   */  
  public String getFileEntry(PageId pageNo, int entryNo)
    throws IOException {

    int position = START_FILE_ENTRIES + entryNo * SIZE_OF_FILE_ENTRY;
    pageNo.pid = Convert.getIntValue (position, data);
    return (Convert.getStrValue (position+4, data, MAX_NAME + 2));
  }
  
}

/**
 * DBFirstPage class which is a subclass of DBHeaderPage class
 */
class DBFirstPage extends DBHeaderPage {

  protected static final int NUM_DB_PAGE = MINIBASE_PAGESIZE -4;
  
  /**
   * Default construtor 
   */
  public DBFirstPage()  { super();}
  
  /**
   * Constructor of class DBFirstPage class
   * @param page a page of Page object
   * @exception IOException I/O errors
   */
  public DBFirstPage(Page page)
    throws IOException	
    {
      super(page, FIRST_PAGE_USED_BYTES);
    }
  
  /** open an exist DB first page
   * @param page a page of Page object
   */
  public void openPage(Page page)
    {
      data = page.getpage();
    }
  
  
  /**
   * set number of pages in the DB
   * @param num the number of pages in DB
   * @exception IOException I/O errors
   */
  public void setNumDBPages(int num)
    throws IOException	
    {
      Convert.setIntValue (num, NUM_DB_PAGE, data);
    }
  
  /**
   * return the number of pages in the DB
   * @return number of pages in DB
   * @exception IOException I/O errors
   */
  public int getNumDBPages()
    throws IOException {

    return (Convert.getIntValue(NUM_DB_PAGE, data));
  }
  
}

/**
 * DBDirectoryPage class which is a subclass of DBHeaderPage class
 */
class DBDirectoryPage extends DBHeaderPage  { //implements PageUsedBytes

  /**
   * Default constructor
   */
  public DBDirectoryPage ()  { super(); }
  
  /**
   * Constructor of DBDirectoryPage class
   * @param page a page of Page object
   * @exception IOException
   */
  public DBDirectoryPage(Page page)
    throws IOException
    {
      super(page, DIR_PAGE_USED_BYTES);
    }
  
  /** open an exist DB directory page
   * @param page a page of Page object
   */
  public void openPage(Page page)
    {
      data = page.getpage();
    }
  
}
