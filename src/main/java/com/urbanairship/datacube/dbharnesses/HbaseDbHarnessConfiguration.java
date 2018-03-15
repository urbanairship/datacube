package com.urbanairship.datacube.dbharnesses;

import com.google.common.base.Preconditions;
import com.urbanairship.datacube.DbHarness;

public class HbaseDbHarnessConfiguration {
    /**
     * Your unique cube name, gets appended to the front of the key.
     */
    public final byte[] uniqueCubeName;

    /**
     * The hbase table name that stores the cube
     */
    public final byte[] tableName;

    /**
     * The hbase column family in which counts are stored
     */
    public final byte[] cf;

    public final DbHarness.CommitType commitType;
    /**
     * The number of threads for flushing batches to hbase
     */
    public final int numFlushThreads;
    public static final int DEFAULT_NUM_FLUSH_THREADS = 5;

    /**
     * How many times we should retry io exceptions
     */
    public final int numIoeTries;
    public static final int DEFAULT_NUM_IO_TRIES = 5;
    /**
     * How many times we should try compare and swaps (we won't apply updates if the existing value is different from
     * expectation)
     */
    public final int numCasTries;
    public static final int DEFAULT_NUM_CAS_TRIES = 10;

    /**
     * A string for disambiguating metrics from this datacube from others
     */
    public final String metricsScope;

    /**
     * The max size of batch database operations, currently only used for deciding how many increments to execute at
     * once.
     */
    public final int batchSize;
    public static final int DEFAULT_BATCH_SIZE = 10;

    public final int idServiceLookupThreads;
    public final static int DEFAULT_ID_SERVICE_LOOKUP_THREADS = 100;

    private HbaseDbHarnessConfiguration(Builder builder) {
        uniqueCubeName = Preconditions.checkNotNull(builder.uniqueCubeName);
        tableName = Preconditions.checkNotNull(builder.tableName);
        cf = Preconditions.checkNotNull(builder.cf);
        commitType = Preconditions.checkNotNull(builder.commitType);
        numFlushThreads = builder.numFlushThreads;
        numIoeTries = builder.numIoeTries;
        numCasTries = builder.numCasTries;
        metricsScope = builder.metricsScope;
        batchSize = builder.batchSize;
        idServiceLookupThreads = builder.idServiceLookupThreads;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private byte[] uniqueCubeName;
        private byte[] tableName;
        private byte[] cf;
        private DbHarness.CommitType commitType;
        private int numFlushThreads = DEFAULT_NUM_FLUSH_THREADS;
        private int numIoeTries = DEFAULT_NUM_IO_TRIES;
        private int numCasTries = DEFAULT_NUM_CAS_TRIES;
        private String metricsScope;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int idServiceLookupThreads = DEFAULT_ID_SERVICE_LOOKUP_THREADS;

        private Builder() {
        }

        public Builder setUniqueCubeName(byte[] uniqueCubeName) {
            this.uniqueCubeName = uniqueCubeName;
            return this;
        }

        public Builder setTableName(byte[] tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setCf(byte[] cf) {
            this.cf = cf;
            return this;
        }

        public Builder setCommitType(DbHarness.CommitType commitType) {
            this.commitType = commitType;
            return this;
        }

        public Builder setNumFlushThreads(int numFlushThreads) {
            this.numFlushThreads = numFlushThreads;
            return this;
        }

        public Builder setNumIoeTries(int numIoeTries) {
            this.numIoeTries = numIoeTries;
            return this;
        }

        public Builder setNumCasTries(int numCasTries) {
            this.numCasTries = numCasTries;
            return this;
        }

        public Builder setMetricsScope(String metricsScope) {
            this.metricsScope = metricsScope;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setIdServiceLookupThreads(int idServiceLookupThreads) {
            this.idServiceLookupThreads = idServiceLookupThreads;
            return this;
        }

        public HbaseDbHarnessConfiguration build() {
            return new HbaseDbHarnessConfiguration(this);
        }
    }
}
