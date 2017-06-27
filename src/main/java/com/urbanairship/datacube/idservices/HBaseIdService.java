/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.idservices;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.math.LongMath;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Util;
import com.urbanairship.datacube.dbharnesses.WithHTable;
import com.urbanairship.datacube.dbharnesses.WithHTable.ScanRunnable;
import com.urbanairship.datacube.metrics.Metrics;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

public class HBaseIdService implements IdService {
    private static final Logger log = LoggerFactory.getLogger(HBaseIdService.class);

    public static final byte[] QUALIFIER = ArrayUtils.EMPTY_BYTE_ARRAY;
    public static final int MAX_COMPAT_TRIES = 20;
    public static final int COMPAT_RETRY_SLEEP = 2000;

    private enum Status {@Deprecated ALLOCATING, ALLOCATED} // don't change ordinals

    private static final byte[] ALLOCATED_BYTES = new byte[]{(byte) Status.ALLOCATED.ordinal()};
    private static final byte[] NULL_BYTE_ARRAY = null;

    private final HTablePool pool;
    private final byte[] counterTable;
    private final byte[] lookupTable;
    private final byte[] uniqueCubeName;
    private final byte[] cf;
    private final Meter allocatingSleeps;
    private final Meter wastedIdNumbers;
    private final Timer idCreationTime;
    private final Timer idGetTime;
    private final Set<Integer> dimensionsApproachingExhaustion;

    public HBaseIdService(Configuration configuration, byte[] lookupTable,
                          byte[] counterTable, byte[] cf, byte[] uniqueCubeName) {
        pool = new HTablePool(configuration, Integer.MAX_VALUE);
        this.lookupTable = lookupTable;
        this.counterTable = counterTable;
        this.uniqueCubeName = uniqueCubeName;
        this.cf = cf;
        this.dimensionsApproachingExhaustion = Sets.newConcurrentHashSet();

        this.allocatingSleeps = Metrics.meter(HBaseIdService.class, "ALLOCATING_sleeps", Bytes.toString(uniqueCubeName));

        this.wastedIdNumbers = Metrics.meter(HBaseIdService.class, "wasted_id_numbers", Bytes.toString(uniqueCubeName));

        this.idCreationTime = Metrics.timer(HBaseIdService.class, "id_create", Bytes.toString(uniqueCubeName));

        this.idGetTime = Metrics.timer(HBaseIdService.class, "id_get", Bytes.toString(uniqueCubeName));

        Metrics.gauge(HBaseIdService.class, "ids_approaching_exhaustion", Bytes.toString(uniqueCubeName), new Gauge<String>() {
            @Override
            public String getValue() {
                return dimensionsApproachingExhaustion.toString();
            }
        });
    }

    @Override
    public byte[] getOrCreateId(int dimensionNum, byte[] input, int numIdBytes) throws IOException,
            InterruptedException {
        Validate.validateDimensionNum(dimensionNum);
        Validate.validateNumIdBytes(numIdBytes);

        // First check if the ID exists already
        final byte[] lookupKey = makeLookupKey(dimensionNum, input);
        final Optional<byte[]> existingId = getId(lookupKey, numIdBytes);
        if (existingId.isPresent()) {
            return existingId.get();
        }

        final Timer.Context timer = idCreationTime.time();
        // If not, we need to create it. First get a new id # from the counter table...
        byte[] counterKey = makeCounterKey(dimensionNum);
        final long id = WithHTable.increment(pool, counterTable, counterKey, cf, QUALIFIER, 1L);

        if (log.isDebugEnabled()) {
            log.debug("Allocated new id " + id);
        }

        final long maxId = LongMath.pow(2L, numIdBytes * 8);
        if (id > maxId) {
            // This dimension has no more IDs available.
            throw new RuntimeException("Exhausted IDs for dimension " + dimensionNum);
        } else if (id < 0) {
            throw new RuntimeException("Somehow ID was less than zero. Weird!");
        } else if (maxId / id <= 2) {
            //If we've used half the keyspace, mark it as approaching exhaustion
            dimensionsApproachingExhaustion.add(dimensionNum);
        }

        // Then try to CAS it into the lookup table. If this fails someone beat us to it, and a
        // get should succeed.
        final Put put = new Put(lookupKey);
        byte[] allocatedRecord = ArrayUtils.addAll(HBaseIdService.ALLOCATED_BYTES, Util.longToBytes(id));
        put.add(cf, QUALIFIER, allocatedRecord);

        boolean swapSuccess = WithHTable.checkAndPut(pool, lookupTable, lookupKey, cf, QUALIFIER, NULL_BYTE_ARRAY, put);
        if (swapSuccess) {
            timer.stop();
            return Util.leastSignificantBytes(id, numIdBytes);
        } else {
            // Someone beat us to creating the mapping, note that and return whatever they inserted.
            wastedIdNumbers.mark();
            final Optional<byte[]> currentValue = getId(lookupKey, numIdBytes);
            timer.stop();
            if (currentValue.isPresent()) {
                return currentValue.get();
            } else {
                throw new RuntimeException("Failed to get id for row " + Bytes.toString(lookupKey) + " after CAS failed");
            }
        }
    }

    @Override
    public Optional<byte[]> getId(int dimensionNum, byte[] input, int numIdBytes) throws IOException, InterruptedException {
        Validate.validateDimensionNum(dimensionNum);
        Validate.validateNumIdBytes(numIdBytes);

        final byte[] lookupkey = makeLookupKey(dimensionNum, input);
        return getId(lookupkey, numIdBytes);
    }

    private Optional<byte[]> getId(byte[] lookupkey, int numIdBytes) throws IOException, InterruptedException {
        final Timer.Context timer = idGetTime.time();
        Result result = WithHTable.get(pool, lookupTable, new Get(lookupkey));
        final byte[] columnVal = result.getValue(cf, QUALIFIER);
        int tries = 0;

        while (tries < MAX_COMPAT_TRIES) {
            if (columnVal != null) {
                int statusOrdinal = columnVal[0];
                Status status = Status.values()[statusOrdinal];

                if (log.isDebugEnabled()) {
                    log.debug("Entry status is " + status);
                }

                switch (status) {
                    case ALLOCATED:
                        byte[] id = Util.trailingBytes(columnVal, numIdBytes);

                        if (log.isDebugEnabled()) {
                            log.debug("Already allocated, returning " + Hex.encodeHexString(id));
                        }

                        timer.stop();
                        return Optional.of(id);

                    case ALLOCATING:
                        //This is here to allow old style writers to coexist. We let them have priority.
                        //If all writers are using this or a newer version, this case should never be hit.
                        allocatingSleeps.mark();
                        Thread.sleep(COMPAT_RETRY_SLEEP);
                        tries++;
                        break;

                    default:
                        throw new RuntimeException("Unexpected column value " +
                                Arrays.toString(columnVal));
                }
            } else {
                timer.stop();
                return Optional.absent();
            }
        }

        throw new RuntimeException(String.format("Row %s was still 'ALLOCATING' after %d millis. Something is wrong.",
                Bytes.toString(lookupkey), MAX_COMPAT_TRIES * COMPAT_RETRY_SLEEP));
    }

    public boolean consistencyCheck() throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(uniqueCubeName);
        scan.setStopRow(ArrayUtils.addAll(uniqueCubeName, new byte[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1}));
        scan.addFamily(cf);
        return WithHTable.scan(pool, lookupTable, scan, new ScanRunnable<Boolean>() {
            @Override
            public Boolean run(ResultScanner rs) {
                boolean anyInconsistenciesFound = false;
                Multimap<Short, Long> sawIds = HashMultimap.create();

                for (Result result : rs) {
                    byte[] rowKey = result.getRow();
                    ByteBuffer bb = ByteBuffer.allocate(2);
                    bb.put(rowKey, uniqueCubeName.length, 2);
                    bb.flip();
                    short dimensionNum = bb.getShort();
                    long id = ByteBuffer.wrap(result.getValue(cf, QUALIFIER)).getLong(1);
                    if (sawIds.containsEntry(dimensionNum, id)) {
                        log.error("Saw a dupe: dimension=" + dimensionNum + " id=" + id);
                        anyInconsistenciesFound = true;
                    } else {
                        log.debug("New value, dimension=" + dimensionNum + " id=" + id);
                        sawIds.put(dimensionNum, id);
                    }
                }

                for (Entry<Short, Collection<Long>> e : sawIds.asMap().entrySet()) {
                    short dimensionNum = e.getKey();
                    Collection<Long> idsThisDimension = e.getValue();

                    long maxId = Long.MIN_VALUE;
                    for (Long id : idsThisDimension) {
                        maxId = Math.max(id, maxId);
                    }

                    if (idsThisDimension.size() != maxId) {
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
            if (idService.consistencyCheck()) {
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
        bb.putShort((short) dimensionNum);
        assert bb.remaining() == 0;
        return bb.array();
    }

    private byte[] makeLookupKey(int dimensionNum, byte[] input) {
        final int bufSize = uniqueCubeName.length + 2 /*dimensionNum*/ + input.length;
        ByteBuffer bb = ByteBuffer.allocate(bufSize);
        bb.put(uniqueCubeName);
        bb.putShort((short) dimensionNum);
        bb.put(input);
        assert bb.remaining() == 0;
        return bb.array();
    }
}
