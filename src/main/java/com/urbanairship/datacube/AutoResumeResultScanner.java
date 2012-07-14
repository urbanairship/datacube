package com.urbanairship.datacube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.regionserver.LeaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ResultScanner that will restart if scan times out on the region servier. This will
 * happen if the client doesn't ask for more results for a certain length of time.
 * 
 * This is particularly useful when doing a merge-join, when one iterator input may go unused for
 * a long time.
 */
@NotThreadSafe
public class AutoResumeResultScanner implements ResultScanner {
    public static final Logger log = LoggerFactory.getLogger(AutoResumeResultScanner.class);
    
    private final HTableInterface hTable;
    private final Scan scan;

    private ResultScanner resultScanner = null;
    private Result[] currentBatch = null;
    private int nextBatchIndex = 0;
    private Result resumeAtResult = null;
    private int numRestarts = 0;

    public AutoResumeResultScanner(HTableInterface hTable, Scan scan) {
        this.hTable = hTable;
        this.scan = scan;
    }

    @Override
    public Iterator<Result> iterator() {
        // Essentially copied from HBase's ClientScanner.iterator()
        return new Iterator<Result>() {
            Result next = null;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    try {
                        next = AutoResumeResultScanner.this.next();
                        return next != null;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    return null;
                }

                Result temp = next;
                next = null;
                return temp;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    
    private int batchSize() {
        return Math.max(1, scan.getCaching());
    }

    /**
     * @return a Result, or null if the scan is finished.
     */
    @Override
    public Result next() throws IOException {
        boolean shouldReadNewBatch = false;
        if(currentBatch == null) {
            // Fresh start, no result batch has yet been loaded. Get one.
            resultScanner = hTable.getScanner(scan);
            currentBatch = new Result[batchSize()];
            shouldReadNewBatch = true;
        } else if (nextBatchIndex == currentBatch.length) {
            // We have a result batch, but we already used them all. Get a new batch.
            shouldReadNewBatch = true;
        }

        if(shouldReadNewBatch) {
            boolean shouldRestartScan = false;
            try {
                currentBatch = resultScanner.next(batchSize());
                nextBatchIndex = 0;
            } catch (LeaseException e) {
                log.warn("Got LeaseException, restarting scan (recoverable)", e);
                shouldRestartScan = true;
            } catch (ScannerTimeoutException e) {
                log.warn("Got ScannerTimeoutException, restarting scan (recoverable)", e);
                shouldRestartScan = true;
            }
            
            if(shouldRestartScan) { 
                // There was a timeout exception while scanning. Get a new ResultScanner starting
                // from the last row seen.
                Scan resumeScan = new Scan(scan);
                if(resumeAtResult != null) {
                    resumeScan.setStartRow(resumeAtResult.getRow());
                }
                
                resultScanner = hTable.getScanner(resumeScan);
                if(resumeAtResult != null) {
                    // We're resuming a scan. Skip the first Result, which we already returned.
                    resultScanner.next();
                }
                nextBatchIndex = 0;
                currentBatch = resultScanner.next(batchSize());
                numRestarts++;
            }
        }
        
        if(nextBatchIndex == currentBatch.length) {
            resultScanner.close();
            return null;
        }
        
        resumeAtResult = currentBatch[nextBatchIndex++]; 
        return resumeAtResult;  
    }
    
    /**
     * @return up to nbRows results, or empty array if scan is finished. Never returns null.
     */
    @Override
    public Result[] next(int nbRows) throws IOException {
        ArrayList<Result> returnList = new ArrayList<Result>(nbRows);
        
        for(int i=0; i<nbRows; i++) {
            Result next = next();
            if(next == null) {
                break;
            }
            returnList.add(next);
        }
        
        return returnList.toArray(new Result[returnList.size()]);
    }

    @Override
    public void close() {
        if(resultScanner != null) {
            resultScanner.close();
        }
    }
    
    public int getNumRestarts() {
        return numRestarts;
    }
}
