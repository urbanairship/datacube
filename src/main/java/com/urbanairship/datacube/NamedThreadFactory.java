/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * To be used with ThreadPoolExecutors to give threads meaningful names in stack traces.
 */
public class NamedThreadFactory implements ThreadFactory {
    private final ThreadFactory wrappedThreadFactory = Executors.defaultThreadFactory();
    private final String name;

    /**
     * @param threadName the name to be given to all threads created by this factory.
     */
    public NamedThreadFactory(String threadName) {
        this.name = threadName;
    }


    @Override
    public Thread newThread(Runnable r) {
        Thread thread = wrappedThreadFactory.newThread(r);
        thread.setName(name);
        return thread;
    }
}
