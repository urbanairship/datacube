package com.urbanairship.datacube.backfill;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface HBaseBackfillCallback {
    public void backfillInto(Configuration conf, byte[] table, byte[] cf, 
            long snapshotFinishMs) throws IOException;
}
