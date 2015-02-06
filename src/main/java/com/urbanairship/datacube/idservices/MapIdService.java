/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.idservices;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;
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

    private final Map<Integer, Map<BoxedByteArray, Long>> idMap = Maps.newHashMap();
    private final Map<Integer, Long> nextIds = Maps.newHashMap();
    private final Map<Integer, Map<Long, BoxedByteArray>> reverseIdMap = Maps.newHashMap();
    private final boolean storeReverseMapping;

    public MapIdService() {
        this.storeReverseMapping = false;
    }

    public MapIdService(boolean storeReverseMapping) {
        this.storeReverseMapping = storeReverseMapping;
    }

    @Override
    public byte[] getId(int dimensionNum, byte[] bytes, int numBytes) {
        Validate.validateDimensionNum(numBytes);
        Validate.validateNumIdBytes(numBytes);

        Map<BoxedByteArray, Long> idMapForDimension = idMap.get(dimensionNum);
        Map<Long, BoxedByteArray> reverseIdMapForDimension = reverseIdMap.get(dimensionNum);

        if (idMapForDimension == null) {
            // This is the first request for this dimension. Create a new map for the dimension.
            if (log.isDebugEnabled()) {
                log.debug("Creating new id map for dimension " + dimensionNum);
            }
            idMapForDimension = Maps.newHashMap();
            idMap.put(dimensionNum, idMapForDimension);

            if (storeReverseMapping) {
                log.debug("Storing Reverse Mapping is enabled for dimensionNum {}", dimensionNum);
                reverseIdMapForDimension = Maps.newHashMap();
                reverseIdMap.put(dimensionNum, reverseIdMapForDimension);
            } else {
                log.debug("Storing Reverse Mapping is disabled for dimensionNum {}", dimensionNum);
            }
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

            if (storeReverseMapping) {
                reverseIdMapForDimension.put(id, inputBytes);
                log.debug("Storing reverse unique ID " + Bytes.toStringBinary(inputBytes.bytes) +
                        " for dimension " + dimensionNum + " and input: " + id);
            }
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

    @Override
    public byte[] getCoordinate(int dimensionNum, byte[] byteArray) {
        long bucketLong = Util.bytesToLong(byteArray);
        Map<Long, BoxedByteArray> mapforDimension = reverseIdMap.get(dimensionNum);
        if (mapforDimension != null) {
            BoxedByteArray value = mapforDimension.get(bucketLong);
            return value.bytes;
        } else {
            log.error("Reverse mapping for dimension "+dimensionNum+ " is not stored" +
                    " so coordinate can not be extracted from MapIdService");
            throw new RuntimeException("Reverse mapping for dimension "+dimensionNum+
                    " is not stored so coordinate can not be extracted from MapIdService");
        }
    }

    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
            outputStream.writeInt(MapIdService.class.getName().length());
            outputStream.write(MapIdService.class.getName().getBytes());
            outputStream.writeBoolean(storeReverseMapping);
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            log.error("Exception in MapIdService serialization "+e);
            throw new RuntimeException(e);
        }
    }

    public static MapIdService deserialize(byte[] bytes) {
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        DataInputStream inputStream = new DataInputStream(input);
        try {
            boolean storeReverseMapping = inputStream.readBoolean();
            return new MapIdService(storeReverseMapping);
        }
        catch (Exception e) {
            log.error("Exception in MapIdSerive deserialization is "+e);
            throw new RuntimeException(e);
        }
    }
}
