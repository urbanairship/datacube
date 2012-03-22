package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
    
    public final static byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY;
    
    public HBaseDbHarness(Configuration hadoopConf, byte[] uniqueCubeName, byte[] tableName, 
            byte[] cf, Deserializer<T> deserializer, IdService idService) throws IOException{
        pool = new HTablePool(hadoopConf, Integer.MAX_VALUE);
        this.deserializer = deserializer;
        this.uniqueCubeName = uniqueCubeName;
        this.tableName = tableName;
        this.cf = cf;
        this.idService = idService;
    }

    
    @Override
    public Optional<T> get(Address c) throws IOException {
        final byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, c.toKey(idService));
        
        Result result = WithHTable.get(pool, tableName, new Get(rowKey));
        if(result == null || result.isEmpty()) {
            log.debug("Returning absent for address " + c);
            return Optional.absent();
        } else {
            T deserialized = deserializer.fromBytes(result.value());
            log.debug("Returning key for address " + c + ": " + deserialized);
            return Optional.of(deserialized);
        }
    }

    @Override
    public void runBatch(Batch<T> batch) throws IOException {
        Map<Address,T> batchMap = batch.getMap();
        
        for(Map.Entry<Address,T> entry: batchMap.entrySet()) {
            writeOne(entry.getKey(), entry.getValue());
        }
    }
    
    public void writeOne(Address address, T op) throws IOException{
        byte[] rowKey = ArrayUtils.addAll(uniqueCubeName, address.toKey(idService));

        Optional<T> preexistingValInDb = get(address);

        T valToWrite;
        if(preexistingValInDb.isPresent()) {
            valToWrite = (T)(preexistingValInDb.get().add(op));
        } else {
            valToWrite = op;
        }
        
        Put put = new Put(rowKey);
        put.add(cf, QUALIFIER, valToWrite.serialize());
        WithHTable.put(pool, tableName, put);
    }
    
    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        throw new NotImplementedException();
    }
}
