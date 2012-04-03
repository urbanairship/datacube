package com.urbanairship.datacube;

import com.google.common.base.Optional;

public interface RollupFilter {
    /**
     * A low-level hook for intercepting writes after bucketing before they are applied to the
     * cube. This is intended to support unique counts; an implementation might provide a
     * RollupFilter implementation that checks whether a given user has already been counted.
     * @param address one of the after-bucketing addresses in the cube that will receive a write.
     * This address consists of one or more (bucketType,bucket) pairs.
     * @param attachment if the writer passed an object to 
     * {@link WriteBuilder#attachForRollupFilter(RollupFilter, Object)}, it will be passed to
     * the RollupFilter. This is a good way to provide a userid to check for uniqueness, for
     * example.
     */
    public boolean filter(Address address, Optional<Object> attachment);
}
