/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.idservices;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Util;

/**
 * An implementation of IdService that's backed by an in-memory map instead of a database.
 * This is useful for testing.
 */
public class MapIdService implements IdService {
    private static final Logger log = LoggerFactory.getLogger(MapIdService.class);
    
    private final Map<Integer,Map<BoxedByteArray,Long>> idMap = Maps.newHashMap();
    private final Map<Integer,Long> nextIds = Maps.newHashMap();
    
    @Override
    public byte[] getId(int dimensionNum, byte[] bytes, int numBytes) {
        Validate.validateDimensionNum(numBytes);
        Validate.validateNumIdBytes(numBytes);
        
        Map<BoxedByteArray,Long> idMapForDimension = idMap.get(dimensionNum);
        
        if(idMapForDimension == null) {
            // This is the first request for this dimension. Create a new map for the dimension.
            if(log.isDebugEnabled()) {
                log.debug("Creating new id map for dimension " + dimensionNum);
            }
            idMapForDimension = Maps.newHashMap();
            idMap.put(dimensionNum, idMapForDimension);
        }
        
        BoxedByteArray inputBytes = new BoxedByteArray(bytes);
        Long id = idMapForDimension.get(inputBytes);
        
        if(id == null) { 
            // We have never seen this input before. Assign it a new ID. 
            id = nextIds.get(dimensionNum);
            if(id == null) {
                // We've never assigned an ID for this dimension+length. Start at 0.
                id = 0L;
            }
 
            // Remember this ID assignment, future requests should get the same ID
            idMapForDimension.put(inputBytes, id);
 
            // The next ID assigned for this dimension should be one greater than this one
            long nextId = id+1L;
            nextIds.put(dimensionNum, nextId);
        }
        
        byte[] idBytesNotTruncated = Util.longToBytes(id);
        byte[] idBytesTruncated = Arrays.copyOfRange(idBytesNotTruncated, 8-numBytes, 8);
        assert Util.bytesToLongPad(idBytesNotTruncated) == Util.bytesToLongPad(idBytesTruncated);
        assert idBytesTruncated.length == numBytes;
        
        if(log.isDebugEnabled()) {
            log.debug("Returning unique ID " + Hex.encodeHexString(idBytesTruncated) + 
                    " for dimension " + dimensionNum + " input " + Hex.encodeHexString(bytes));
        }
        return idBytesTruncated;
    }
}
