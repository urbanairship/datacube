/*
Copyright 2012 Urban Airship and Contributors
*/

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
        // If you're debugging a map or reduce, you'll have to hardcode this to true, because the
        // system properties won't be passed on to the tasktracker JVMs.
//        isEnabled = true;
        isEnabled = System.getProperty("ua.test.integration", "").toLowerCase().equals("true")
                && System.getProperty("ua.test.debughack", "").toLowerCase().equals("true");
    }
    
    public static boolean isEnabled() {
        return isEnabled;
    }
    
    /**
     * Log a message.
     * 
     * Note that this will probably be a no-op in a map or reduce task unless you find some way to set
     * the required system properties on those JVMs to enable this code.
     */
    public static void log(String s) {
        if(isEnabled) {
            hacklog.HackLog.log(s);
        }
    }
}
