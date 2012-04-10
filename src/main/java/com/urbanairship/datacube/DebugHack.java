package com.urbanairship.datacube;

/**
 * Since debugging map and reduce tasks is difficult or impossible, this class provides a hacky way 
 * to log to a single file on the filesystem.
 * 
 * Only use this in a test environment.
 */
public class DebugHack {
    private static final boolean isEnabled;
    
    static {
        isEnabled = System.getProperty("ua.test.integration", "").toLowerCase().equals("true")
                && System.getProperty("ua.test.debughack", "").toLowerCase().equals("true");
    }
    
    public static boolean isEnabled() {
        return isEnabled;
    }
    
    public static void log(String s) {
        if(isEnabled) {
            hacklog.HackLog.log(s);
        }
    }
}
