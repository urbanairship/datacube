package com.urbanairship.datacube;

import java.util.Map;

public interface Bucketer {
    Map<BucketType,byte[]> getBuckets(Object coord);
}
