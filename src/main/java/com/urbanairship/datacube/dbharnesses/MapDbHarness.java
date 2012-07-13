/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Op;

/**
 * For testing, this is is a backing store for a cube that lives in memory. It saves us from 
 * calling a DB just to test the cube logic.
 */
public class MapDbHarness<T extends Op> implements DbHarness<T> {
    private final static Logger log = LoggerFactory.getLogger(MapDbHarness.class);

    private static final int casRetries = 10;
    private static final Future<?> nullFuture = new NullFuture();
    
    private final ConcurrentMap<BoxedByteArray,byte[]> map;
    private final Deserializer<T> deserializer;
    private final CommitType commitType;
    private final IdService idService;
    
    public MapDbHarness(ConcurrentMap<BoxedByteArray,byte[]> map, Deserializer<T> deserializer, 
            CommitType commitType, IdService idService) {
        this.map = map;
        this.deserializer = deserializer;
        this.commitType = commitType;
        this.idService = idService;
        if(commitType != CommitType.OVERWRITE && commitType != CommitType.READ_COMBINE_CAS) {
            throw new IllegalArgumentException("MapDbHarness doesn't support commit type " + 
                    commitType);
        }
    }
    
    /**
     * Actually synchronous and not asyncronous, which is allowed.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Future<?> runBatchAsync(Batch<T> batch, AfterExecute<T> afterExecute) {
        
        for(Map.Entry<Address,T> entry: batch.getMap().entrySet()) {
            Address address = entry.getKey();
            T opFromBatch = entry.getValue();

            BoxedByteArray mapKey;
            try {
                mapKey = new BoxedByteArray(address.toKey(idService));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            
            if(commitType == CommitType.READ_COMBINE_CAS) {
                int casRetriesRemaining = casRetries;
                do {
                    Optional<byte[]> oldBytes;
                    try {
                        oldBytes = getRaw(address);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    
                    T newOp;
                    if(oldBytes.isPresent()) {
                        T oldOp = deserializer.fromBytes(oldBytes.get());
                        newOp = (T)opFromBatch.add(oldOp);
                        if(log.isDebugEnabled()) {
                            log.debug("Combined " + oldOp + " and " +  opFromBatch + " into " +
                                    newOp);
                        }
                    } else {
                        newOp = opFromBatch;
                    }
                    
                    if(oldBytes.isPresent()) {
                        // Compare and swap, if the key is still mapped to the same byte array.
                        if(map.replace(mapKey, oldBytes.get(), newOp.serialize())) {
                            if(log.isDebugEnabled()) {
                                log.debug("Successful CAS overwrite at key " + 
                                        Hex.encodeHexString(mapKey.bytes)  + " with " + 
                                        Hex.encodeHexString(newOp.serialize()));
                            }
                            break;
                        }
                    } else {
                        // Compare and swap, if the key is still absent from the map.
                        if(map.putIfAbsent(mapKey, newOp.serialize()) == null) {
                            // null is returned when there was no existing mapping for the 
                            // given key, which is the success case.
                            if(log.isDebugEnabled()) {
                                log.debug("Successful CAS insert without existing key for key " + 
                                        Hex.encodeHexString(mapKey.bytes) + " with " + 
                                        Hex.encodeHexString(newOp.serialize()));
                            }
                            break;
                        }
                    }
                } while (casRetriesRemaining-- > 0);
                if(casRetriesRemaining == -1) {
                    RuntimeException e = new RuntimeException("CAS retries exhausted");
                    afterExecute.afterExecute(e);
                    throw e;
                }
            } else if(commitType == CommitType.OVERWRITE) {
                map.put(mapKey, opFromBatch.serialize());
                if(log.isDebugEnabled()) {
                    log.debug("Write of key " + Hex.encodeHexString(mapKey.bytes));
                }
            } else {
                throw new AssertionError("Unsupported commit type: " + commitType);
            }
        }
        batch.reset();
        afterExecute.afterExecute(null); // null throwable => success
        return nullFuture;
    }

    @Override
    public Optional<T> get(Address address) throws IOException, InterruptedException {
        Optional<byte[]> bytes = getRaw(address);
        if(bytes.isPresent()) {
            return Optional.of(deserializer.fromBytes(bytes.get()));
        } else {
            return Optional.absent();
        }
    }
    
    @Override
    public void flush() throws InterruptedException {
        return; // all ops are synchronously applied, nothing to do
    }

    private Optional<byte[]> getRaw(Address address) throws InterruptedException {
        byte[] mapKey;
        try {
            mapKey = address.toKey(idService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        byte[] bytes = map.get(new BoxedByteArray(mapKey));
        if(log.isDebugEnabled()) {
            log.debug("getRaw for key " + Hex.encodeHexString(mapKey) + " returned " + 
                    Arrays.toString(bytes));
        }
        if(bytes == null) {
            return Optional.absent();
        } else {
            return Optional.of(bytes);
        }
    }

    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        throw new NotImplementedException();
    }
    
    private static class NullFuture implements Future<Object> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return null;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }
        
    }
}
