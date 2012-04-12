package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Op;

public class HBaseDbHarness<T extends Op> implements DbHarness<T> {
    
    private static final Logger log = LogManager.getLogger(HBaseDbHarness.class);
    
    private final HTablePool pool;
    private final Deserializer<T> deserializer;
    private final byte[] uniqueCubeName;
    private final byte[] tableName;
    private final byte[] cf;
    private final IdService idService;
    private final int maxBatchSize;
    
    public final static byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY;
    
    /**
     * @param maxBatchSize when storing a large number of ops to the DB, we'll handle them in 
     * batches of this many at a time. We get one batch worth of existing ops from the DB,
     * combine them with the outgoing ops, then flush the combined ops in a batch write.
     */
    public HBaseDbHarness(Configuration hadoopConf, byte[] uniqueCubeName, byte[] tableName, 
            byte[] cf, Deserializer<T> deserializer, IdService idService,
            int maxBatchSize) throws IOException{
        pool = new HTablePool(hadoopConf, Integer.MAX_VALUE);
        this.deserializer = deserializer;
        this.uniqueCubeName = uniqueCubeName;
        this.tableName = tableName;
        this.cf = cf;
        this.idService = idService;
        this.maxBatchSize = maxBatchSize;
    }

    
    @Override
    public Optional<T> get(Address c) throws IOException {
        final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, c.toKey(idService));
        
        Get get = new Get(rowKey);
        get.addFamily(cf);
        Result result = WithHTable.get(pool, tableName, get);
        if(result == null || result.isEmpty()) {
            log.debug("Returning absent for address " + c + " key " + 
                    Base64.encodeBase64String(rowKey));
            return Optional.absent();
        } else {
            T deserialized = deserializer.fromBytes(result.value());
            log.debug("Returning value for address " + c + ": " + " key " +
                    Base64.encodeBase64String(rowKey) + ": " + deserialized);
            return Optional.of(deserialized);
        }
    }

    @Override
    public void runBatch(Batch<T> batch) throws IOException {
        Map<Address,T> batchMap = batch.getMap();
        
        // In batches of HBASE_BATCH_SIZE, we'll get the existing values from the DB, combine them
        // with the values to be written, and flush the new values to the DB.
        List<Address> succesfullyWritten = new ArrayList<Address>(batch.getMap().size());

        int hbaseBatchSize = Math.min(batch.getMap().size(), maxBatchSize);
        
        try {
            List<T> ops = new ArrayList<T>(hbaseBatchSize);
            List<Get> gets = new ArrayList<Get>(hbaseBatchSize);
            List<Address> addresses = new ArrayList<Address>(hbaseBatchSize);
            
            for(Map.Entry<Address,T> entry: batchMap.entrySet()) {
                Address address = entry.getKey();
                T op = entry.getValue();
        
                byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, address.toKey(idService));
                addresses.add(address);
                ops.add(op);
                
                log.debug("Queueing a read for " + Base64.encodeBase64String(rowKey));
                Get get = new Get(rowKey);
                get.addFamily(cf);
                gets.add(get);
                
                if(gets.size() == hbaseBatchSize) {
                    doGetMergeWrite(gets, ops);
                    // If the get-merge-write was successful, remember the addresses that succeeded so
                    // they can be removed from the batch later.
                    succesfullyWritten.addAll(addresses);
                    
                    ops = new ArrayList<T>(hbaseBatchSize);
                    gets = new ArrayList<Get>(hbaseBatchSize);
                    addresses = new ArrayList<Address>(hbaseBatchSize);
                }
            }
            if(gets.size() != 0) {
                doGetMergeWrite(gets, ops);
            }
            batch.reset(); // All operations succeeded. Empty out the batch.
        } catch (IOException e) {
            // There was an IOException while attempting to use the DB. If we were successful with some
            // of the pending writes, they should be removed from the batch so they are not retried later.
            // The operations that didn't get into the DB should be left in the batch to be retried later.
            for(Address address: succesfullyWritten) {
                batch.getMap().remove(address);
            }
        }
    }

    /**
     * Used as a subroutine by {@link #runBatch(Batch)}. It gets old values from the database,
     * combines them with new values to be written, and flushes the combined values back to the DB.
     * @param gets gets that will retrieve the old values from the database. ORDER IS IMPORTANT, the
     * order of the gets is assumed to match the order of the ops; it is assumed that get[i] retrieves
     * the value to be combined with ops[i].
     * @param ops the new ops that should be combined with the values already in the DB. ORDER IS IMPORTANT, the
     * order of the gets is assumed to match the order of the ops; it is assumed that get[i] retrieves
     * the value to be combined with ops[i].
     */
    @SuppressWarnings("unchecked")
    private void doGetMergeWrite(List<Get> gets, List<T> ops) throws IOException {
        Result[] previousValues = WithHTable.get(pool, tableName, gets);

        if(gets.size() != ops.size() || gets.size() != previousValues.length) {
            throw new RuntimeException("Input list sizes should have been equal");
        }


        Iterator<Result> resultIter = new ArrayIterator(previousValues);
        Iterator<T> opIter = ops.iterator();
        Iterator<Get> getIter = gets.iterator();
        
        List<Row> writeBatch = new ArrayList<Row>(ops.size());
        
        while(opIter.hasNext()) {
            // Since we're traversing all the iterators in order, all of these locals
            // are for the same single cell update.
            T newOp = opIter.next();
            Result result = resultIter.next();
            Get get = getIter.next();
            
            byte[] rowKey = get.getRow();
            
            T combinedOp;
            byte[] prevSerializedOp = result.getValue(cf, QUALIFIER);
            if(prevSerializedOp == null) {
                log.debug("At row " + Base64.encodeBase64String(rowKey) + " using new value " +
                        newOp);
                combinedOp = newOp;
            } else {
                combinedOp = (T)deserializer.fromBytes(prevSerializedOp).add(newOp);
                log.debug("At row " + Base64.encodeBase64String(rowKey) + " using combined value " +
                        combinedOp);
            }
            
            Put put = new Put(rowKey);
            put.add(cf, QUALIFIER, combinedOp.serialize());
            writeBatch.add(put);
        }
        
        WithHTable.batch(pool, tableName, writeBatch);
    }
    
    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        throw new NotImplementedException();
    }
}
