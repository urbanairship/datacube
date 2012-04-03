package com.urbanairship.datacube.backfill;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;

public class BackfillUtil {
    /**
     * Get the non-null non-null non-zero-length row keys that divide the regions
     */
    public static byte[][] getSplitKeys(Pair<byte[][],byte[][]> regionStartsEnds) {
        byte[][] starts = regionStartsEnds.getFirst();
        
        List<byte[]> splitKeys = new ArrayList<byte[]>();
        for(int i=1; i<starts.length; i++) {
            splitKeys.add(starts[i]);
        }
        return splitKeys.toArray(new byte[][] {});
    }
}
