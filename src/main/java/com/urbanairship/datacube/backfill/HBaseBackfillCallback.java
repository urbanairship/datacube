/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.backfill;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface HBaseBackfillCallback {
    void backfillInto(Configuration conf, byte[] table, byte[] cf, long snapshotFinishMs) throws IOException;
}
