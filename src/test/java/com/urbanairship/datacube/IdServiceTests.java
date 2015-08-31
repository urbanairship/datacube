/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.math.LongMath;
import org.junit.Assert;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class IdServiceTests {
    public static void basicTest(IdService idService) throws Exception {
        final int numFieldBytes = 5;
        
        // Different inputs should always produce different outputs (non-repeating ids)
        Set<BoxedByteArray> idsSeen = new HashSet<BoxedByteArray>();
        for(int i=0; i<500; i++) {
            byte[] newId = idService.getOrCreateId(0, Util.longToBytes(i), numFieldBytes);
            Assert.assertEquals(numFieldBytes, newId.length);
            BoxedByteArray newBox = new BoxedByteArray(newId);
            Assert.assertTrue("ID was repeated: " + newBox, idsSeen.add(newBox));
        }
        
        // The same input should produce the same output
        byte[] id1 = idService.getOrCreateId(1, Util.longToBytes(10), numFieldBytes);
        byte[] id2 = idService.getOrCreateId(1, Util.longToBytes(10), numFieldBytes);
        Assert.assertEquals(numFieldBytes, id1.length);
        Assert.assertArrayEquals(id1, id2);
    }
    
    /**
     * Generating 2^(fieldbits) unique IDs should work, then generating one more should raise an
     * exception because no more IDs were available.
     *  
     */
    public static void testExhaustion(IdService idService, int numFieldBytes, int dimensionNum) 
            throws Exception {
        int numFieldBits = numFieldBytes * 8;
        
        long numToGenerate = LongMath.pow(2, numFieldBits);
        long i=0;
        for(; i<numToGenerate; i++) {
            byte[] id = idService.getOrCreateId(dimensionNum, Util.longToBytes(i), numFieldBytes);
            Assert.assertEquals(numFieldBytes, id.length);
        }
        
        try {
            idService.getOrCreateId(dimensionNum, Util.longToBytes(i), numFieldBytes);
            Assert.fail("getOrCreateId call should have thrown an exception");
        } catch (RuntimeException e) {
            // Happy success
        }
        
        // Subsequent calls for the same input should fail quickly (and not block for long)
        long startTimeNanos = System.nanoTime();
        try {
            idService.getOrCreateId(dimensionNum, Util.longToBytes(i), numFieldBytes);
            Assert.fail("ID allocation should have failed");
        } catch(RuntimeException e) {
            if(System.nanoTime() - startTimeNanos > TimeUnit.SECONDS.toNanos(5)) {
                Assert.fail("Took too long to fail");
            }
        }
    }
}
