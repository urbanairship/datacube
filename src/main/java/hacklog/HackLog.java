package hacklog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Since debugging map and reduce tasks is difficult or impossible, this class provides a hacky way 
 * to log to a single file on the filesystem.
 * 
 * Only use this in a test environment.
 */
public class HackLog {
    public static void log(String s) {
        try {
            RandomAccessFile raf = new RandomAccessFile(new File("/tmp/hacklog.txt"), "rwd");
            raf.seek(raf.length());
            raf.write((s + "\n").getBytes());
            raf.close();
        } catch (IOException e) {
            System.err.println("HackLog IOException!");
        }
    }
}
