/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.idservices;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.math.LongMath;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Util;
import com.urbanairship.datacube.dbharnesses.WithHTable;
import com.urbanairship.datacube.dbharnesses.WithHTable.ScanRunnable;

public class HBaseIdService implements IdService {
    private static final Logger log = LoggerFactory.getLogger(HBaseIdService.class);
    
    public static final byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY;
    public static final byte[] REVERSE_QUALIFIER = new byte[] {'r'};
    public static final long ALLOC_TIMEOUT_MS = 10000;

    private static enum Status {ALLOCATING, ALLOCATED}; // don't change ordinals
    private static final byte[] ALLOCATING_BYTES = new byte[] {(byte)Status.ALLOCATING.ordinal()}; 
    
    private final HTablePool pool;
    private final byte[] counterTable;
    private final byte[] lookupTable;
    private final byte[] uniqueCubeName;
    private final byte[] cf;
    private final boolean storeReverseMapping;
    private final String zookeperQuorum;
    private final String clientPort;
    private final String parentNode;

    public HBaseIdService(Configuration configuration, byte[] lookupTable,
            byte[] counterTable, byte[] cf, byte[] uniqueCubeName, boolean storeReverseMapping) {
        pool = new HTablePool(configuration, Integer.MAX_VALUE);
        this.lookupTable = lookupTable;
        this.counterTable = counterTable;
        this.uniqueCubeName = uniqueCubeName;
        this.cf = cf;
        this.storeReverseMapping = storeReverseMapping;
        this.zookeperQuorum = configuration.get(HConstants.ZOOKEEPER_QUORUM);
        this.clientPort = configuration.get(HConstants.CLIENT_PORT_STR);
        this.parentNode = configuration.get(HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }

    public HBaseIdService(Configuration configuration, byte[] lookupTable,
            byte[] counterTable, byte[] cf, byte[] uniqueCubeName) {
        this(configuration, lookupTable, counterTable, cf, uniqueCubeName, false);
    }

    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
            outputStream.writeInt(HBaseIdService.class.getName().length());
            outputStream.write(HBaseIdService.class.getName().getBytes());

            outputStream.writeInt(zookeperQuorum.length());
            outputStream.write(zookeperQuorum.getBytes());

            outputStream.writeInt(clientPort.length());
            outputStream.write(clientPort.getBytes());

            outputStream.writeInt(parentNode.length());
            outputStream.write(parentNode.getBytes());

            outputStream.writeInt(lookupTable.length);
            outputStream.write(lookupTable);
            outputStream.writeInt(counterTable.length);
            outputStream.write(counterTable);
            outputStream.writeInt(cf.length);
            outputStream.write(cf);
            outputStream.writeInt(uniqueCubeName.length);
            outputStream.write(uniqueCubeName);
            outputStream.writeBoolean(storeReverseMapping);
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            log.error("Exception in HbaseIdService serialization is " + e);
            throw new RuntimeException(e);
        }
    }

    public static HBaseIdService deserialize(byte[] bytes) {
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        DataInputStream inputStream = new DataInputStream(input);
        try {
            int zookeperLength = inputStream.readInt();
            byte[] zookeperBytes = new byte[zookeperLength];
            inputStream.readFully(zookeperBytes, 0, zookeperLength);

            int clientPortLength = inputStream.readInt();
            byte[] clientPortBytes = new byte[clientPortLength];
            inputStream.readFully(clientPortBytes, 0, clientPortLength);

            int parentNodeLength = inputStream.readInt();
            byte[] parentNodeBytes = new byte[parentNodeLength];
            inputStream.readFully(parentNodeBytes, 0, parentNodeLength);

            int lookUpTableSize = inputStream.readInt();
            byte[] lookUpTable = new byte[lookUpTableSize];
            inputStream.readFully(lookUpTable, 0, lookUpTableSize);

            int counterTableSize = inputStream.readInt();
            byte[] counterTable = new byte[counterTableSize];
            inputStream.readFully(counterTable, 0, counterTableSize);

            int cfSize = inputStream.readInt();
            byte[] cf = new byte[cfSize];
            inputStream.readFully(cf, 0, cfSize);

            int uniqueCubeNameSize = inputStream.readInt();
            byte[] uniqueCubeName = new byte[uniqueCubeNameSize];
            inputStream.readFully(uniqueCubeName, 0, uniqueCubeNameSize);

            boolean storeReverseMapping = inputStream.readBoolean();

            Configuration configuration = HBaseConfiguration.create();
            log.debug("ZOOKEPER QUORUM used in HbaseIdService recreation is {}",
                    Bytes.toString(zookeperBytes));
            log.debug("ZOOKEPER CLIENT PORT used in HbaseIdService recreation is {}",
                    Bytes.toString(clientPortBytes));
            log.debug("ZOOKEPER ZNODE PARENT used in HbaseIdService recreation is {}",
                    Bytes.toString(parentNodeBytes));

            configuration.set(HConstants.ZOOKEEPER_QUORUM, Bytes.toString(zookeperBytes));
            configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, Bytes.toString(clientPortBytes));
            configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, Bytes.toString(parentNodeBytes));

            return new HBaseIdService(configuration, lookUpTable, counterTable, cf, uniqueCubeName,
                    storeReverseMapping);
        }
        catch (Exception e) {
            log.error("Exception in HbaseIdService deserialization is "+e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getId(int dimensionNum, byte[] input, int numIdBytes) throws IOException,
            InterruptedException {
        Validate.validateDimensionNum(dimensionNum);
        Validate.validateNumIdBytes(numIdBytes);

        final byte[] lookupKey = makeLookupKey(dimensionNum, input);
        log.debug("LookUpKey from makeLookUpKey {}", Bytes.toStringBinary(lookupKey));

        /*
         * PHASE 1
         * 
         * Look up the (status,id) for this input. If there is already an id for this
         * input, return it. Otherwise:
         * 
         * Case: If there is a record showing that another thread is currently trying to allocate 
         * an id for this input, sleep and try again soon.
         * 
         * Case: If there is a record showing that another thread tried to allocate an id for
         * this input a long time ago, assume that it failed and replace it with our own record
         * showing that we're attempting to allocate an id for this input.
         * 
         * Case: If there are no records showing that any other thread has attempt to allocate
         * an id for this input, insert a record showing that we're trying to allocate an id.
         */
        byte[] allocRecord;
        while(true) {
            Result result = WithHTable.get(pool, lookupTable, new Get(lookupKey));
            final byte[] columnVal = result.getValue(cf, QUALIFIER);
            if(columnVal != null) {
                int statusOrdinal = columnVal[0];
                Status status = Status.values()[statusOrdinal];
                if(log.isDebugEnabled()) {
                    log.debug("Entry status is " + status);
                }
                switch(status) {
                case ALLOCATED:
                    byte[] id = Util.trailingBytes(columnVal, numIdBytes);
                    if(log.isDebugEnabled()) {
                        log.debug("Already allocated, returning " + Hex.encodeHexString(id));
                    }
                    return id; 
                case ALLOCATING:
                    long allocTimestamp = result.getColumn(cf, QUALIFIER).get(0).getTimestamp();
                    long msSinceAlloc = System.currentTimeMillis() - allocTimestamp;
                    
                    if(msSinceAlloc < ALLOC_TIMEOUT_MS) {
                        // Another thread is already allocating an id for this tag. Wait.
                        if(log.isDebugEnabled()) {
                            log.debug("Waiting for other thread to finish allocating id");
                        }
                        Thread.sleep(500);
                        continue;
                    } else {
                        log.warn("Preempting expired allocator for input " + 
                                Base64.encodeBase64String(input));
                    }
                    break;
                default:
                    throw new RuntimeException("Unexpected column value " + 
                            Arrays.toString(columnVal));
                }
            }
            // Either (1) there is no entry for this tag, or (2) there was an ALLOCATING
            // placeholder for this tag, but it has expired. checkAndPut the new value
            // into place.

            final Put put = new Put(lookupKey);
            byte[] nanoStamp = Util.longToBytes(System.nanoTime());
            allocRecord = ArrayUtils.addAll(ALLOCATING_BYTES, nanoStamp);
            put.add(cf, QUALIFIER, allocRecord);
            
            boolean swapSuccess = WithHTable.checkAndPut(pool, lookupTable, lookupKey, cf, 
                    QUALIFIER, columnVal, put);
            if(swapSuccess) {
                if(log.isDebugEnabled()) {
                    log.debug("Allocation record CAS success");
                }
                break;
            }
            if(log.isDebugEnabled()) {
                log.debug("Allocation record CAS failed, retrying");
            }

        }

        /*
         * PHASE 2
         * 
         * We've successfully inserted a record showing that we're going to allocate an ID for
         * this input. This should temporarily exclude other threads from trying to do the
         * same. This exclusion prevents unused IDs from being allocated if multiple threads
         * allocate an ID concurrently.
         * 
         * Now we assign an ID by taking the next key of the counter for this dimension.
         */
        byte[] counterKey = makeCounterKey(dimensionNum);
        final long id = WithHTable.increment(pool, counterTable, counterKey, cf, QUALIFIER, 1L);
        if(log.isDebugEnabled()) {
            log.debug("Allocated new id " + id);
        }
        
        final long maxId = LongMath.pow(2L, numIdBytes * 8);
        if(id > maxId) {
            // This dimension has no more IDs available. We'll throw an exception, but first we'll
            // remove the "ALLOCATING" record so future attempts can fail quickly.
            // If this CAS fails, there's another concurrent thread trying to allocate an ID for
            // the same input, but there's nothing we can do about it.
            WithHTable.checkAndDelete(pool, lookupTable, lookupKey, cf, QUALIFIER, 
                    allocRecord, new Delete(lookupKey));
            throw new RuntimeException("Exhausted IDs for dimension " + dimensionNum);
        }
        if(id < 0) {
            throw new RuntimeException("Somehow ID was less than zero. Weird!");
        }
        
        /*
         * PHASE 3
         * 
         * We have a unique value to be used as an id value. Now we persist the mapping from
         * input -> id. 
         * 
         * This write is done as a checkAndPut to handle the rare possibility that
         * another thread preempted our allocation record and allocated an ID. This could 
         * occur if our thread suffered a long GC pause or other slowness. It's critical that
         * we don't replace an existing valid mapping, since we must have complete consistency
         * of IDs: once one thread uses an input->id mapping, every other thread must use it
         * forever.
         */

        final int revBufSize = uniqueCubeName.length + 2;
        ByteBuffer bb = ByteBuffer.allocate(revBufSize);
        bb.put(uniqueCubeName);
        bb.putShort((short) dimensionNum);
        byte[] revArray = bb.array();
        byte[] allocateRevKey = ArrayUtils.addAll(revArray, Util.longToBytes(id));
        final Put putRev = new Put(allocateRevKey);
        putRev.add(cf, REVERSE_QUALIFIER, lookupKey);


        final Put put = new Put(lookupKey);
        byte[] allocatedRecord = ArrayUtils.addAll(new byte[] {(byte)Status.ALLOCATED.ordinal()}, 
                Util.longToBytes(id));
        put.add(cf, QUALIFIER, allocatedRecord);
        boolean swapSuccess = WithHTable.checkAndPut(pool, lookupTable, lookupKey, cf, QUALIFIER, 
                allocRecord, put);
        if (swapSuccess) {
            if (storeReverseMapping) {
                log.debug("Storing Reverse Mapping is enabled for dimensionNum {}", dimensionNum);
                WithHTable.put(pool, lookupTable, putRev);
                log.debug("Reverse mapping key in "+Bytes.toStringBinary(lookupTable)+
                        " table is "+Bytes.toStringBinary(allocateRevKey)+" for dimension "
                        +dimensionNum+" input "+Bytes.toStringBinary(lookupKey));
            } else {
                log.debug("Storing Reverse Mapping is disabled for dimensionNum {}", dimensionNum);
            }
            return Util.leastSignificantBytes(id, numIdBytes);
        } else {
            log.warn("Concurrent allocators!?!? ID " + id + " will never be used");
            // Recurse, try again.
            return getId(dimensionNum, input, numIdBytes);
        }
    }

    @Override
    public byte[] getCoordinate(int dimensionNum, byte[] byteArray) {
        if (storeReverseMapping == false) {
            log.error("As storing reverse mapping is disabled so can not get " +
                    "coordinate for dimension "+dimensionNum);
            throw new RuntimeException("As storing reverse mapping is disabled so can " +
                    "not get coordinate for dimension "+dimensionNum);
        }

        final int bufSize = uniqueCubeName.length + 2;
        ByteBuffer bb = ByteBuffer.allocate(bufSize);
        bb.put(uniqueCubeName);
        bb.putShort((short) dimensionNum);
        byte[] lookUpKeyFirstPart = bb.array();
        byte[] generatedlookUpKey = ArrayUtils.addAll(lookUpKeyFirstPart, byteArray);

        log.debug("Reverse lookUpKey: {} in table {} when getting coordinate from HbaseIdService",
                Bytes.toStringBinary(generatedlookUpKey), Bytes.toStringBinary(lookupTable));

        try {
            Get get = new Get(generatedlookUpKey);
            Result result = WithHTable.get(pool, lookupTable, get);
            final byte[] columnVal = result.getValue(cf, REVERSE_QUALIFIER);

            if (columnVal == null) {
                log.error("The value stored at REVERSE_QUALIFIER for Reverse LookUpKey "
                        +Bytes.toStringBinary(generatedlookUpKey)+" in "+
                        Bytes.toStringBinary(lookupTable)+ " table is null");
                throw new RuntimeException("The value stored at REVERSE_QUALIFIER for " +
                        "Reverse LookUpKey "+Bytes.toStringBinary(generatedlookUpKey)+" in "+
                        Bytes.toStringBinary(lookupTable)+ " table is null");
            } else {
                int uniqueCubeLength = uniqueCubeName.length;
                int prefixLength = uniqueCubeLength + 2;
                byte[] actualDimensionValue = ArrayUtils.subarray(columnVal, prefixLength,
                        columnVal.length);
                log.debug("Actual coordinate is {}", Bytes.toStringBinary(actualDimensionValue)+
                " for input "+Bytes.toStringBinary(byteArray));
                return actualDimensionValue;
            }
        } catch (Exception e) {
            log.error("Exception present in getting coordinate from HBaseIdService " + e);
            throw new RuntimeException(e);
        }
    }

    public boolean consistencyCheck() throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(uniqueCubeName);
        scan.setStopRow(ArrayUtils.addAll(uniqueCubeName, new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1}));
        scan.addFamily(cf);
        return WithHTable.scan(pool, lookupTable, scan, new ScanRunnable<Boolean>() {
            @Override
            public Boolean run(ResultScanner rs) {
                boolean anyInconsistenciesFound = false;
                Multimap<Short,Long> sawIds = HashMultimap.create();
                
                for(Result result: rs) {
                    byte[] rowKey = result.getRow();
                    ByteBuffer bb = ByteBuffer.allocate(2);
                    bb.put(rowKey, uniqueCubeName.length, 2);
                    bb.flip();
                    short dimensionNum = bb.getShort();
                    long id = ByteBuffer.wrap(result.getValue(cf, QUALIFIER)).getLong(1);
                    if(sawIds.containsEntry(dimensionNum, id)) {
                        log.error("Saw a dupe: dimension=" + dimensionNum + " id=" + id);
                        anyInconsistenciesFound = true;
                    } else {
                        log.debug("New value, dimension=" + dimensionNum + " id=" + id);
                        sawIds.put(dimensionNum, id);
                    }
                }
                
                for(Entry<Short,Collection<Long>> e: sawIds.asMap().entrySet()) {
                    short dimensionNum = e.getKey();
                    Collection<Long> idsThisDimension = e.getValue();
                    
                    long maxId = Long.MIN_VALUE;
                    for(Long id: idsThisDimension) {
                        maxId = Math.max(id, maxId);
                    }
                    
                    if(idsThisDimension.size() != maxId) {
                        log.error("Some ids were missing in dimension " + dimensionNum);
                        anyInconsistenciesFound = true;
                    }
                }
                
                return !anyInconsistenciesFound;
            }
        });
    }
    
    /**
     * This class gives users a way to run a consistency check from the command line.
     */
    public static class ConsistencyCheck {
        public static void main(String[] args) throws Exception {
            byte[] lookupTable = args[0].getBytes();
            byte[] counterTable = args[1].getBytes();
            byte[] cf = args[2].getBytes();
            byte[] uniqueCubeName = args[3].getBytes();
            
            Configuration conf = HBaseConfiguration.create(); // parse XML configs on classpath
            HBaseIdService idService = new HBaseIdService(conf, lookupTable, counterTable, cf, uniqueCubeName);
            if(idService.consistencyCheck()) {
                log.info("Check passed");
                System.exit(0);
            } else {
                log.warn("Check failed");
                System.exit(1);
            }
        }
    }

    private byte[] makeCounterKey(int dimensionNum) {
        final int bufSize = uniqueCubeName.length + 2; // 2 for dimensionNum 
        ByteBuffer bb = ByteBuffer.allocate(bufSize);
        bb.put(uniqueCubeName);
        bb.putShort((short)dimensionNum);
        assert bb.remaining() == 0;
        return bb.array();
    }
    
    private byte[] makeLookupKey(int dimensionNum, byte[] input) {
        final int bufSize = uniqueCubeName.length + 2 /*dimensionNum*/ + input.length; 
        ByteBuffer bb = ByteBuffer.allocate(bufSize);
        bb.put(uniqueCubeName);
        bb.putShort((short)dimensionNum);
        bb.put(input);
        assert bb.remaining() == 0;
        return bb.array();
    }
}
