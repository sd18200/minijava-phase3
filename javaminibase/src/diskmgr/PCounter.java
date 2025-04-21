package diskmgr;

public class PCounter {
    private static int readCount = 0;
    private static int writeCount = 0;

    public static void initialize() {
        readCount = 0;
        writeCount = 0;
    }

    public static void incrementReadCount() {
        readCount++;
    }

    public static void incrementWriteCount() {
        writeCount++;
    }

    public static int getRCount() {
        return readCount;
    }

    public static int getWCount() {
        return writeCount;
    }
}