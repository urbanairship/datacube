/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.dbharnesses;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WithHTable {
    private static final Logger log = LoggerFactory.getLogger(WithHTable.class);
    
    /**
     * Take an htable from the pool, use it with the given HTableRunnable, and return it to
     * the pool. This is the "loan pattern" where the htable resource is used temporarily by
     * the runnable.
     */
    public static <T> T run(HTablePool pool, byte[] tableName, HTableRunnable<T> runnable) 
            throws IOException {
        HTableInterface hTable = null;
        try {
            hTable = pool.getTable(tableName);
            return runnable.runWith(hTable);
        } catch(Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            if(hTable != null) {
                pool.putTable(hTable);
            }
        }
    }
    
    public static interface HTableRunnable<T> {
        public T runWith(HTableInterface hTable) throws IOException;
    }

    /**
     * Do an HBase put and return null.
     * @throws IOException if the underlying HBase operation throws an IOException
     */
    public static void put(HTablePool pool, byte[] tableName, final Put put) throws IOException {
        run(pool, tableName, new HTableRunnable<Object>() {
            @Override
            public Object runWith(HTableInterface hTable) throws IOException {
                hTable.put(put);
                return null;
            }
        });
    }

    public static Result get(HTablePool pool, byte[] tableName, final Get get) throws IOException {
        return run(pool, tableName, new HTableRunnable<Result>() {
            @Override
            public Result runWith(HTableInterface hTable) throws IOException {
                return hTable.get(get);
            }
        });
    }

    public static long increment(HTablePool pool, byte[] tableName, final byte[] row,
            final byte[] cf, final byte[] qual, final long amount) throws IOException {
        return run(pool, tableName, new HTableRunnable<Long> () {
            @Override
            public Long runWith(HTableInterface hTable) throws IOException {
                return hTable.incrementColumnValue(row, cf, qual, amount);
            }
        });
    }
    
    public static boolean checkAndPut(HTablePool pool, byte[] tableName, final byte[] row,
            final byte[] cf, final byte[] qual, final byte[] value, final Put put) 
                    throws IOException {
        return run(pool, tableName, new HTableRunnable<Boolean>() {
            @Override
            public Boolean runWith(HTableInterface hTable) throws IOException {
                return hTable.checkAndPut(row, cf, qual, value, put);
            }
        });
    }

    public static boolean checkAndDelete(HTablePool pool, byte[] tableName, final byte[] row,
            final byte[] cf, final byte[] qual, final byte[] value, final Delete delete) 
                    throws IOException {
        return run(pool, tableName, new HTableRunnable<Boolean>() {
            @Override
            public Boolean runWith(HTableInterface hTable) throws IOException {
                return hTable.checkAndDelete(row, cf, qual, value, delete);
            }
        });
    }
    
    static class WrappedInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public final InterruptedException wrappedException;
        
        public WrappedInterruptedException(InterruptedException ie) {
            this.wrappedException = ie;
        } 
    }
    
    public static Object[] batch(HTablePool pool, byte[] tableName, final List<Row> actions) 
            throws IOException, InterruptedException {
        try {
            return run(pool, tableName, new HTableRunnable<Object[]>() {
                @Override
                public Object[] runWith(HTableInterface hTable) throws IOException {
                    try {
                        return hTable.batch(actions);
                    } catch (InterruptedException e) {
                        throw new WrappedInterruptedException(e);
                    }
                }
            });
        } catch (WrappedInterruptedException e) {
            throw e.wrappedException;
        }
    }
    
    public static Result[] get(HTablePool pool, byte[] tableName, final List<Get> gets) 
            throws IOException {
        return run(pool, tableName, new HTableRunnable<Result[]>() {
            @Override
            public Result[] runWith(HTableInterface hTable) throws IOException {
                return hTable.get(gets);
            }
        });
    }

    public static interface ScanRunnable<T> {
        public T run(ResultScanner rs) throws IOException;
    }
    
    public static <T> T scan(HTablePool pool, byte[] tableName, final Scan scan, 
            final ScanRunnable<T> scanRunnable) throws IOException {
        return run(pool, tableName, new HTableRunnable<T>() {
            @Override
            public T runWith(HTableInterface hTable) throws IOException {
                ResultScanner rs = null;
                try {
                    rs = hTable.getScanner(scan);
                    return scanRunnable.run(rs);
                } finally {
                    if(rs != null) {
                        rs.close();
                    }
                }
            }
        });
    }
}
