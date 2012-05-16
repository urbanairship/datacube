package com.urbanairship.datacube;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class AutoResumeResultScannerTest extends EmbeddedClusterTestAbstract {
    
    private static final String tableName = "myTable";
    private static final String cfName = "myCf";
    
    @BeforeClass
    public static void skipUnlessPropSet() throws Exception {
        // Since this test takes a long time, skip it unless a certain system property is set.
        String shouldRun = System.getProperty("datacube.runAutoResumeResultScannerTest", "false");
        Assume.assumeTrue(Boolean.valueOf(shouldRun));
    }
    
    private static HTable createSchemaAndInsertTestData() throws Exception {
        HTable hTable;
        if(getTestUtil().getHBaseAdmin().tableExists(tableName.getBytes())) {
            hTable = new HTable(getTestUtil().getConfiguration(), tableName);
        } else { 
            hTable = getTestUtil().createTable(tableName.getBytes(), cfName.getBytes());
        }
        
        for(int i=0; i<5; i++) {
            byte[] iAsBytes = Bytes.toBytes(i);
            Put put = new Put(iAsBytes);
            put.add(cfName.getBytes(), iAsBytes, iAsBytes);
            hTable.put(put);
        }
        return hTable;
    }
    
    @Test
    public void testWithTimeouts() throws Exception {
        HTable hTable = createSchemaAndInsertTestData();
        
        Scan scan = new Scan();
        scan.addFamily(cfName.getBytes());
        AutoResumeResultScanner scanner = new AutoResumeResultScanner(hTable, scan);
        
        Assert.assertEquals(0, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(1, Bytes.toInt(scanner.next().value()));
        
        // Wait for 70 seconds between 2nd the 3rd next calls. This should cause the regionserver
        // to time out the scanner 
        Assert.assertEquals(0, scanner.getNumRestarts());
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(2, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(1, scanner.getNumRestarts());
        
        // Pause and make the scanner resume yet again.
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(3, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(2, scanner.getNumRestarts());
        Assert.assertEquals(4, Bytes.toInt(scanner.next().value()));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
        
        // Test that resuming a finished scan doesn't cause weirdness
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
    }
    
    @Test
    public void normalScanTest() throws Exception {
        HTable hTable = createSchemaAndInsertTestData();
        
        Scan scan = new Scan();
        scan.addFamily(cfName.getBytes());
        AutoResumeResultScanner scanner = new AutoResumeResultScanner(hTable, scan);
        
        Assert.assertEquals(0, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(1, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(2, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(3, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(4, Bytes.toInt(scanner.next().value()));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
        
        Assert.assertEquals(0, scanner.getNumRestarts());
    }
    
    /**
     * Test using next(int), and not just next()
     */
    @Test
    public void testMultiGet() throws Exception {
        HTable hTable = createSchemaAndInsertTestData();
        
        Scan scan = new Scan();
        scan.addFamily(cfName.getBytes());
        AutoResumeResultScanner scanner = new AutoResumeResultScanner(hTable, scan);
        Result[] batch;
        
        batch = scanner.next(2);
        Assert.assertEquals(0, Bytes.toInt(batch[0].value()));
        Assert.assertEquals(1, Bytes.toInt(batch[1].value()));
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        batch = scanner.next(2);
        Assert.assertEquals(2, Bytes.toInt(batch[0].value()));
        Assert.assertEquals(1, scanner.getNumRestarts());
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(3, Bytes.toInt(batch[1].value()));
        Assert.assertEquals(1, scanner.getNumRestarts());
        
        batch = scanner.next(2);
        Assert.assertEquals(4, Bytes.toInt(batch[0].value()));
        Assert.assertEquals(1, batch.length);
        Assert.assertNull(scanner.next());  // Scan finished, should be null
        Assert.assertEquals(2, scanner.getNumRestarts());
    }

    /**
     * Test that things still work when the Scan uses setCaching() > 1.
     */
    @Test
    public void testScanCaching() throws Exception {
        HTable hTable = createSchemaAndInsertTestData();
        
        Scan scan = new Scan();
        scan.setCaching(3);
        scan.addFamily(cfName.getBytes());
        AutoResumeResultScanner scanner = new AutoResumeResultScanner(hTable, scan);
        
        Assert.assertEquals(0, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(1, Bytes.toInt(scanner.next().value()));
        
        // Wait for 70 seconds between 2nd the 3rd next calls. This should cause the regionserver
        // to time out the scanner 
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(2, Bytes.toInt(scanner.next().value()));
        
        // Pause and make the scanner resume yet again.
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(3, Bytes.toInt(scanner.next().value()));
        Assert.assertEquals(4, Bytes.toInt(scanner.next().value()));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
        
        // Test that resuming a finished scan doesn't cause weirdness
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
    }
    
    @Test
    public void testIterator() throws Exception {
        HTable hTable = createSchemaAndInsertTestData();

        Scan scan = new Scan();
        scan.setCaching(3);
        scan.addFamily(cfName.getBytes());
        AutoResumeResultScanner scanner = new AutoResumeResultScanner(hTable, scan);
        Iterator<Result> it = scanner.iterator();
        
        it.hasNext();
        Assert.assertEquals(0, Bytes.toInt(it.next().value()));
        it.hasNext();
        Assert.assertEquals(1, Bytes.toInt(it.next().value()));
        
        // Wait for 70 seconds between 2nd the 3rd next calls. This should cause the regionserver
        // to time out the scanner 
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(2, Bytes.toInt(it.next().value()));
        
        // Pause and make the scanner resume yet again.
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertEquals(3, Bytes.toInt(it.next().value()));
        Assert.assertEquals(4, Bytes.toInt(it.next().value()));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
        
        // Test that resuming a finished scan doesn't cause weirdness
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        Assert.assertNull(scanner.next());  // Scan finished, should be null
    }
}
