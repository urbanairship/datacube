/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class is mostly intended for internal use by datacube code. By using this class directly you
 * can skip the bucketers and manipulate individual cube values without higher-level magic.
 * <p/>
 * If you're just trying to use the datacube normally, check out {@link DataCubeIo}, {@link ReadBuilder}
 */
public class Address {
//    private static final Logger log = LogManager.getLogger(Address.class);

    private final Map<Dimension<?>, BucketTypeAndBucket> buckets = Maps.newHashMap();
    private final DataCube<?> cube;

    private static final byte[] WILDCARD_FIELD = new byte[]{0};
    private static final byte[] NON_WILDCARD_FIELD = new byte[]{1};

    public Address(DataCube<?> cube) {
        this.cube = cube;
    }

    public void at(Dimension<?> dimension, byte[] value) {
        if (dimension.isBucketed()) {
            throw new IllegalArgumentException("Dimension " + dimension +
                    " is a bucketed dimension. You can't query it without a bucket.");
        }
        at(dimension, BucketType.IDENTITY, value);
    }

    public void at(Dimension<?> dimension, BucketType bucketType, byte[] bucket) {
        buckets.put(dimension, new BucketTypeAndBucket(bucketType, bucket));
    }

    public void at(Dimension<?> dimension, BucketTypeAndBucket bucketAndCoord) {
        buckets.put(dimension, bucketAndCoord);
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
        List<Dimension<?>> dimensions = cube.getDimensions();

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
                    int dimensionNum = cube.getDimensions().indexOf(dimension);
                    if (readOnly) {
                        final Optional<byte[]> maybeId =
                                idService.getId(dimensionNum, bucketAndCoord.bucket, dimension.getNumFieldBytes());

                        if (maybeId.isPresent()) {
                            elem = maybeId.get();
                        } else {
                            return Optional.absent();
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
        if (this.cube.useAddressPrefixByteHash()) {
            bb = ByteBuffer.allocate(totalKeySize + 1);
            bb.put((byte) 0x01);
        } else {
            bb = ByteBuffer.allocate(totalKeySize);
        }

        for (byte[] keyElement : keyElemsInOrder) {
            bb.put(keyElement);
        }

        // Update the byte prefix placeholder of the hash of the key contents if required.
        if (this.cube.useAddressPrefixByteHash()) {
            byte hashByte = Util.hashByteArray(bb.array(), 1, totalKeySize + 1);
            bb.put(0, hashByte);
        }

        if (bb.remaining() != 0) {
            throw new AssertionError("Key length calculation was somehow wrong, " +
                    bb.remaining() + " bytes remaining");
        }
        return Optional.of(bb.array());
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        boolean firstLoop = true;
        for (Entry<Dimension<?>, BucketTypeAndBucket> e : buckets.entrySet()) {
            if (!firstLoop) {
                sb.append(", ");
            }
            firstLoop = false;
            Dimension<?> dimension = e.getKey();
            sb.append(dimension);
            sb.append(": ");
            sb.append(e.getValue());
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Eclipse auto-generated
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((buckets == null) ? 0 : buckets.hashCode());
        result = prime * result + ((cube == null) ? 0 : cube.hashCode());
        return result;
    }

    /**
     * Eclipse auto-generated
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Address other = (Address) obj;
        if (buckets == null) {
            if (other.buckets != null)
                return false;
        } else if (!buckets.equals(other.buckets))
            return false;
        if (cube == null) {
            if (other.cube != null)
                return false;
        } else if (!cube.equals(other.cube))
            return false;
        return true;
    }
}
