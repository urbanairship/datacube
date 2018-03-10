/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class is mostly intended for internal use by datacube code. By using this class directly you
 * can skip the bucketers and manipulate individual cube values without higher-level magic.
 *
 * If you're just trying to use the datacube normally, check out {@link DataCubeIo}, {@link ReadBuilder}
 */
public class Address {
    private final Map<Dimension<?, ?>, BucketTypeAndBucket> buckets = Maps.newHashMap();

    private final boolean usePartitionByte;
    private final List<Dimension<?, ?>> dimensions;

    public static Address create(DataCube<?> cube) {
        return new Address(cube.useAddressPrefixByteHash(), cube.getDimensions());
    }

    public Address(boolean usePartitionByte, List<Dimension<?, ?>> dimensions) {
        this.usePartitionByte = usePartitionByte;
        this.dimensions = dimensions;
    }

    public Address at(Dimension<?, ?> dimension, byte[] value) {
        if (dimension.isBucketed()) {
            throw new IllegalArgumentException("Dimension " + dimension +
                    " is a bucketed dimension. You can't query it without a bucket.");
        }
        at(dimension, BucketType.IDENTITY, value);
        return this;
    }

    public Address at(Dimension<?, ?> dimension, BucketType bucketType, byte[] bucket) {
        buckets.put(dimension, new BucketTypeAndBucket(bucketType, bucket));
        return this;
    }

    public Address at(Dimension<?, ?> dimension, BucketTypeAndBucket bucketAndCoord) {
        buckets.put(dimension, bucketAndCoord);
        return this;
    }

    public BucketTypeAndBucket get(Dimension<?, ?> dimension) {
        return buckets.get(dimension);
    }

    public Map<Dimension<?, ?>, BucketTypeAndBucket> getBuckets() {
        return buckets;
    }

    public byte[] toWriteKey(IdService idService) throws IOException, InterruptedException {
        final Optional<byte[]> maybeKey = toKey(idService, false);
        if (!maybeKey.isPresent()) {
            throw new RuntimeException("Unexpected failure to create key for write for " + this.toString());
        }
        return maybeKey.get();
    }

    public Optional<byte[]> toReadKey(IdService idService) throws IOException, InterruptedException {
        return toKey(idService, true);
    }

    /**
     * Get a byte array encoding the buckets of this cell in the Cube. For internal use only.
     * If readOnly is true, then absent will be returned if any dimension fails to map. Callers
     * should consider this evidence that the key does not exist in the backing store.
     */
    private Optional<byte[]> toKey(IdService idService, boolean readOnly) throws IOException, InterruptedException {
        OffsetBasedAddressFormat offsetBasedAddressFormat = new OffsetBasedAddressFormat(dimensions, usePartitionByte, idService, Address::new);
        return offsetBasedAddressFormat.toKey(this, readOnly);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("buckets", buckets)
                .add("usePartitionByte", usePartitionByte)
                .add("dimensions", dimensions)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Address)) return false;
        Address address = (Address) o;
        return usePartitionByte == address.usePartitionByte &&
                Objects.equal(buckets, address.buckets) &&
                Objects.equal(dimensions, address.dimensions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(buckets, usePartitionByte, dimensions);
    }
}
