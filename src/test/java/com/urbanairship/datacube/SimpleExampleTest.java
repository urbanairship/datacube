package com.urbanairship.datacube;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;


public class SimpleExampleTest {

    /**
     * Test the core datacube logic. Don't use a database, use our in-memory pretend storage
     * backend.
     */
    @Test
    public void writeAndRead() throws Exception {
        IdService idService = new CachingIdService(5, new MapIdService());
        ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
                new ConcurrentHashMap<BoxedByteArray,byte[]>();
        
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, 
                LongOp.DESERIALIZER, CommitType.READ_COMBINE_CAS, 3, idService);
        
        DbHarnessTests.basicTest(dbHarness);
    }
}
