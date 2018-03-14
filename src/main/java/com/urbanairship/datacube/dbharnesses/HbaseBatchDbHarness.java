package com.urbanairship.datacube.dbharnesses;

import com.codahale.metrics.Histogram;
import com.google.common.collect.ImmutableMap;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.metrics.Metrics;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class HbaseBatchDbHarness implements BatchDbHarness {
    private final byte[] uniqueCubeName;

    private final IdService idService;
    private final BiConsumer<Map<Address, LongOp>, IOException> idLookupFailureConsumer;
    private final BiConsumer<Map<byte[], Long>, IOException> incrementFailure;

    private final Histogram attempts;
    private final Histogram failedIncrementOperations;
    private final Histogram ioexceptions;
    private BlockingIO<Map<byte[], Long>, Map<byte[], Long>> incrementer;

    public static HbaseBatchDbHarness create(
            byte[] uniqueCubeName,
            byte[] tableName,
            byte[] cf,
            String metricsScope,
            IdService idService,
            HTablePool pool
    ) {
        return new HbaseBatchDbHarness(
                metricsScope,
                uniqueCubeName,
                new HbaseBatchIncrementer(cf, metricsScope, pool, tableName),
                idService,
                (map, ioe) -> log.error("Failed lookup for " + map + " after retries", ioe),
                (map, ioe) -> log.error("Failed lookup for increment " + map + " after retries", ioe)
        );
    }

    public HbaseBatchDbHarness(String metricName, byte[] uniqueCubeName, BlockingIO<Map<byte[], Long>, Map<byte[], Long>> incrementer, IdService idService, BiConsumer<Map<Address, LongOp>, IOException> idLookupFailureConsumer, BiConsumer<Map<byte[], Long>, IOException> incrementFailure) {
        this.uniqueCubeName = uniqueCubeName;
        this.idService = idService;
        this.incrementer = incrementer;
        this.idLookupFailureConsumer = idLookupFailureConsumer;
        this.incrementFailure = incrementFailure;

        attempts = Metrics.histogram(BatchDbHarness.class, "attempts", metricName);
        failedIncrementOperations = Metrics.histogram(BatchDbHarness.class, "failedIncrementOperations", metricName);
        ioexceptions = Metrics.histogram(BatchDbHarness.class, "IOException", metricName);
    }

    private static final Logger log = LogManager.getLogger(HbaseBatchDbHarness.class);

    private <V, R> R retryIo(RetryPolicy retryPolicy, BlockingIO<V, R> io, V input, int hash) throws IOException, InterruptedException {
        int failures = 0;
        boolean retriesExhausted = false;
        while (!retriesExhausted) {
            try {
                return io.apply(input);
            } catch (IOException exception) {
                failures++;
                retriesExhausted = retryPolicy.sleep(failures);
                log.error(String.format("idlookup attempts %s for batch with hashcode %s failed", exception, hash));
            } finally {
                ioexceptions.update(failures);
            }
        }
        throw new IOException("io operation retries exhausted before success for " + hash);
    }

    @Override
    public void increment(Map<Address, LongOp> batch, RetryPolicy retryPolicy) throws InterruptedException, IOException {
        int hash = batch.hashCode();
        MDC.put("batch", String.valueOf(hash));

        Map<byte[], Long> writes = new HashMap<>();
        for (Map.Entry<Address, LongOp> entry : batch.entrySet()) {
            try {
                byte[] rowKey = this.retryIo(retryPolicy, this::addressToRowKey, entry.getKey(), hash);
                long increment = entry.getValue().getLong();
                writes.put(rowKey, increment);
            } catch (IOException ioe) {
                idLookupFailureConsumer.accept(ImmutableMap.<Address, LongOp>builder().put(entry).build(), ioe);
            }
        }

        int attempts = 0;
        boolean retriesExhausted = false;

        while (!retriesExhausted) {
            if (writes.isEmpty()) {
                return;
            }
            try {
                writes = this.retryIo(retryPolicy, incrementer, writes, hash);
                if (writes.isEmpty()) {
                    break;
                }
                failedIncrementOperations.update(writes.size());
                attempts++;
                retriesExhausted = retryPolicy.sleep(attempts);
            } catch (IOException ioe) {
                incrementFailure.accept(writes, ioe);
                break;
            }
        }

        if (!writes.isEmpty()) {
            incrementFailure.accept(writes, null);
        }

        this.attempts.update(attempts);
        MDC.clear();
    }

    private byte[] addressToRowKey(Address a) throws IOException, InterruptedException {
        return ArrayUtils.addAll(uniqueCubeName, a.toWriteKey(idService));
    }

}

