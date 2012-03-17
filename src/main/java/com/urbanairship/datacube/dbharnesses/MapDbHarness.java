package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.CasRetriesExhausted;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Op;

/**
 * For testing, this is is a backing store for a cube that lives in memory. It saves us from 
 * calling a DB just to test the cube logic.
 */
public class MapDbHarness<T extends Op> implements DbHarness<T> {
    private final static Logger log = LogManager.getLogger(MapDbHarness.class);
    
    private final List<Dimension<?>> dimensions;
    private final ConcurrentMap<BoxedByteArray,byte[]> map;
    private final Deserializer<T> deserializer;
    private final CommitType commitType;
    private final int casRetries;
    private final IdService idService;
    
    public MapDbHarness(List<Dimension<?>> dimensions, ConcurrentMap<BoxedByteArray,byte[]> map, 
            Deserializer<T> deserializer, CommitType commitType, int casRetries, 
            IdService idService) {
        this.dimensions = dimensions;
        this.map = map;
        this.deserializer = deserializer;
        this.commitType = commitType;
        this.casRetries = casRetries;
        this.idService = idService;
        if(commitType != CommitType.OVERWRITE && commitType != CommitType.READ_COMBINE_CAS) {
            throw new IllegalArgumentException("MapDbHarness doesn't support commit type " + 
                    commitType);
        }
    }
    @Override
    public void runBatch(Batch<T> batch) throws IOException {
        
        for(Map.Entry<Address,T> entry: batch.getMap().entrySet()) {
            Address address = entry.getKey();
            T opFromBatch = entry.getValue();

            BoxedByteArray mapKey = new BoxedByteArray(address.toKey(dimensions, idService));
            
            if(commitType == CommitType.READ_COMBINE_CAS) {
                int casRetriesRemaining = casRetries;
                do {
                    Optional<byte[]> oldBytes = getRaw(address);
                    
                    T newOp;
                    if(oldBytes.isPresent()) {
                        T oldOp = deserializer.fromBytes(oldBytes.get());
                        newOp = (T)opFromBatch.combine(oldOp);
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
                                log.debug("Successful CAS insert without existing value for key " + 
                                        Hex.encodeHexString(mapKey.bytes) + " with " + 
                                        Hex.encodeHexString(newOp.serialize()));
                            }
                            break;
                        }
                    }
                } while (casRetriesRemaining-- > 0);
                if(casRetriesRemaining == -1) {
                    throw new CasRetriesExhausted();
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
    }

    @Override
    public Optional<T> get(Address address) throws IOException {
        Optional<byte[]> bytes = getRaw(address);
        if(bytes.isPresent()) {
            return Optional.of(deserializer.fromBytes(bytes.get()));
        } else {
            return Optional.absent();
        }
    }
    
    private Optional<byte[]> getRaw(Address address) throws IOException {
        byte[] mapKey = address.toKey(dimensions, idService);
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

}
