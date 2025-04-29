package global;

import java.io.IOException; // Import IOException
// import java.io.FileNotFoundException; // Not strictly needed if catching IOException
import bufmgr.*;
import diskmgr.*;
import catalog.*;

public class SystemDefs {
  public static BufMgr	JavabaseBM;
  public static DB	JavabaseDB;
  public static Catalog	JavabaseCatalog;

  public static String  JavabaseDBName;
  public static String  JavabaseLogName;
  public static boolean MINIBASE_RESTART_FLAG = false;
  public static String	MINIBASE_DBNAME;

  public SystemDefs (){};

  // Modify constructor signature
  public SystemDefs(String dbname, int num_pgs, int bufpoolsize,
            String replacement_policy )
    throws IOException, // Add exceptions thrown by init
           InvalidPageNumberException,
           FileIOException,
           DiskMgrException,
           PageNotFoundException,
           HashEntryNotFoundException, // Add others as needed based on BufMgr implementation
           PagePinnedException,
           PageUnpinnedException,
           InvalidFrameNumberException,
           HashOperationException,
           BufMgrException
    {
      int logsize;

      String real_logname = new String(dbname);
      String real_dbname = new String(dbname);

      if (num_pgs == 0) {
        logsize = 500;
      }
      else {
        logsize = 3*num_pgs;
      }

      if (replacement_policy == null) {
        replacement_policy = new String("Clock");
      }

      init(real_dbname,real_logname, num_pgs, logsize,
           bufpoolsize, replacement_policy);
    }


  // Modify init method signature
  public void init( String dbname, String logname,
            int num_pgs, int maxlogsize,
            int bufpoolsize, String replacement_policy )
    throws IOException, // Add exceptions thrown by DB.openDB
           InvalidPageNumberException,
           FileIOException,
           DiskMgrException,
           PageNotFoundException,
           HashEntryNotFoundException, // Add others as needed based on BufMgr implementation
           PagePinnedException,
           PageUnpinnedException,
           InvalidFrameNumberException,
           HashOperationException,
           BufMgrException
    {

      // boolean status = true; // Not used
      JavabaseBM = null;
      JavabaseDB = null;
      JavabaseDBName = null;
      JavabaseLogName = null;
      JavabaseCatalog = null;

      try {
        JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
        JavabaseDB = new DB();
/*
        JavabaseCatalog = new Catalog();
*/
      }
      // Keep this catch for BufMgr/DB constructor errors, but don't exit for DB.openDB errors
      catch (Exception e) { // Catch specific BufMgr/DB constructor exceptions if possible
        System.err.println ("Error initializing buffer manager or DB object: "+e);
        e.printStackTrace();
        // Consider re-throwing a specific initialization error instead of exiting
        throw new RuntimeException("Initialization failed", e); // Or handle differently
        // Runtime.getRuntime().exit(1); // Avoid exiting here if possible
      }

      JavabaseDBName = new String(dbname);
      JavabaseLogName = new String(logname);
      MINIBASE_DBNAME = new String(JavabaseDBName);

      // create or open the DB

      // *** REMOVE the try-catch blocks around DB calls ***
      if ((MINIBASE_RESTART_FLAG)||(num_pgs == 0)){ //open an existing database
        // try { // REMOVE try
          JavabaseDB.openDB(dbname); // Let exceptions propagate
        // } // REMOVE catch
        // catch (Exception e) { // REMOVE catch
        //   System.err.println (""+e);
        //   e.printStackTrace();
        //   Runtime.getRuntime().exit(1);
        // }
      }
      else { // create a new database
        // try { // REMOVE try
          JavabaseDB.openDB(dbname, num_pgs);
          JavabaseBM.flushAllPages(); // Flush after creation seems reasonable
        // } // REMOVE catch
        // catch (Exception e) { // REMOVE catch
        //   System.err.println (""+e);
        //   e.printStackTrace();
        //   Runtime.getRuntime().exit(1);
        // }
      }
    }
}