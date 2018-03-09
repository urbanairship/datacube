/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    private final Map<Dimension<?>, BucketTypeAndBucket> buckets = Maps.newHashMap();

    private static final byte[] WILDCARD_FIELD = new byte[]{0};
    private static final byte[] NON_WILDCARD_FIELD = new byte[]{1};
    private final boolean usePartitionByte;
    private final List<Dimension<?>> dimensions;

    public static Address create(DataCube<?> cube) {
        return new Address(cube.useAddressPrefixByteHash(), cube.getDimensions());
    }

    public Address(boolean usePartitionByte, List<Dimension<?>> dimensions) {
        this.usePartitionByte = usePartitionByte;
        this.dimensions = dimensions;
    }

    public Address at(Dimension<?> dimension, byte[] value) {
        if (dimension.isBucketed()) {
            throw new IllegalArgumentException("Dimension " + dimension +
                    " is a bucketed dimension. You can't query it without a bucket.");
        }
        at(dimension, BucketType.IDENTITY, value);
        return this;
    }

    public Address at(Dimension<?> dimension, BucketType bucketType, byte[] bucket) {
        buckets.put(dimension, new BucketTypeAndBucket(bucketType, bucket));
        return this;
    }

    public Address at(Dimension<?> dimension, BucketTypeAndBucket bucketAndCoord) {
        buckets.put(dimension, bucketAndCoord);
        return this;
    }

    public BucketTypeAndBucket get(Dimension<?> dimension) {
        return buckets.get(dimension);
    }

    public Map<Dimension<?>, BucketTypeAndBucket> getBuckets() {
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
        boolean sawOnlyWildcardsSoFar = true;
        List<byte[]> reversedKeyElems = Lists.newArrayListWithCapacity(dimensions.size());

        // We build up the key in reverse order so we can leave off wildcards at the end of the key.
        // The reasoning for this is complicated, please see design docs.
        for (int i = dimensions.size() - 1; i >= 0; i--) {
            Dimension<?> dimension = dimensions.get(i);
            BucketTypeAndBucket bucketAndCoord = buckets.get(dimension);

            int thisDimBucketLen = dimension.getNumFieldBytes();
            int thisDimBucketTypeLen = dimension.getBucketPrefixSize();

            if (bucketAndCoord == BucketTypeAndBucket.WILDCARD || bucketAndCoord == null) {
                // Special logic, wildcards at the end of the key are omitted
                if (sawOnlyWildcardsSoFar) {
                    continue;
                }
                reversedKeyElems.add(new byte[thisDimBucketTypeLen + thisDimBucketLen]);
                reversedKeyElems.add(WILDCARD_FIELD);
            } else {
                sawOnlyWildcardsSoFar = false;

                byte[] elem;
                if (idService == null || !dimension.getDoIdSubstitution()) {
                    elem = bucketAndCoord.bucket;
                } else {
                    int dimensionNum = dimensions.indexOf(dimension);
                    if (readOnly) {
                        final Optional<byte[]> maybeId =
                                idService.getId(dimensionNum, bucketAndCoord.bucket, dimension.getNumFieldBytes());

                        if (maybeId.isPresent()) {
                            elem = maybeId.get();
                        } else {
                            return Optional.empty();
                        }

                    } else {
                        elem = idService.getOrCreateId(dimensionNum, bucketAndCoord.bucket, dimension.getNumFieldBytes());
                    }
                }

                if (elem.length != thisDimBucketLen) {
                    throw new IllegalArgumentException("Field length was wrong (after bucketing " +
                            " and unique ID substitution). For dimension " + dimension +
                            ", expected length " + dimension.getNumFieldBytes() + " but was " +
                            bucketAndCoord.bucket.length);
                }

                byte[] bucketTypeId = bucketAndCoord.bucketType.getUniqueId();
                if (bucketTypeId.length != thisDimBucketTypeLen) {
                    throw new RuntimeException("Bucket prefix length was wrong. For dimension " +
                            dimension + ", expected bucket prefix of length " + dimension.getBucketPrefixSize() +
                            " but the bucket prefix was " + Arrays.toString(bucketTypeId) +
                            " which had length" + bucketTypeId.length);
                }
                reversedKeyElems.add(elem);
                reversedKeyElems.add(bucketTypeId);
                reversedKeyElems.add(NON_WILDCARD_FIELD);
            }
        }

        List<byte[]> keyElemsInOrder = Lists.reverse(reversedKeyElems);

        int totalKeySize = 0;
        for (byte[] keyElement : keyElemsInOrder) {
            totalKeySize += keyElement.length;
        }
        ByteBuffer bb;
        // Add a place holder for the hash byte if it's required
        if (usePartitionByte) {
            bb = ByteBuffer.allocate(totalKeySize + 1);
            bb.put((byte) 0x01);
        } else {
            bb = ByteBuffer.allocate(totalKeySize);
        }

        for (byte[] keyElement : keyElemsInOrder) {
            bb.put(keyElement);
        }

        // Update the byte prefix placeholder of the hash of the key contents if required.
        if (usePartitionByte) {
            byte hashByte = Util.hashByteArray(bb.array(), 1, totalKeySize + 1);
            bb.put(0, hashByte);
        }

        if (bb.remaining() != 0) {
            throw new AssertionError("Key length calculation was somehow wrong, " +
                    bb.remaining() + " bytes remaining");
        }
        return Optional.of(bb.array());
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
