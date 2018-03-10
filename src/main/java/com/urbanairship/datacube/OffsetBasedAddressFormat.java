package com.urbanairship.datacube;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Maps the supplied address to and from bytes.
 *
 * Byte format:
 * <pre>
 *     {@code
 *      bytes = [OPTIONAL PARTITION BYTE]
 *      for each dimension:
 *              bytes += [WILDCARD OR NAH][DIMENSION BUCKET TYPE BYTE PREFIX][DIMENSION BUCKET VALUE]
 *  }
 * </pre>
 *
 * There are no unique/unambiguous delimiters between components of the rowkey.
 */
public class OffsetBasedAddressFormat implements AddressFormatter {
    private static final byte[] WILDCARD_FIELD = new byte[]{0};
    private static final byte[] NON_WILDCARD_FIELD = new byte[]{1};
    private static final BaseEncoding BASE_64 = BaseEncoding.base64();

    private final IdService idService;
    private final List<Dimension<?>> dimensions;
    private final boolean useAddressPrefixByteHash;
    private final BiFunction<Boolean, List<Dimension<?>>, Address> addressSupplier;

    public OffsetBasedAddressFormat(DataCube<?> cube, IdService idService) {
        this(

                cube.getDimensions(),
                cube.useAddressPrefixByteHash(),
                idService,
                Address::new
        );
    }

    public OffsetBasedAddressFormat(List<Dimension<?>> dimensions, boolean addPartitionByte, IdService idService, BiFunction<Boolean, List<Dimension<?>>, Address> addressSupplier) {
        this.dimensions = dimensions;
        this.useAddressPrefixByteHash = addPartitionByte;
        this.idService = idService;
        this.addressSupplier = addressSupplier;
    }

    /**
     * Get a byte array encoding the buckets of this cell in the Cube. For internal use only.
     * If  doNotCreateIds is true, then absent will be returned if any dimension fails to map. Callers
     * should consider this evidence that the key does not exist in the backing store.
     */
    public Optional<byte[]> toKey(Address address, boolean doNotCreateIds) throws IOException, InterruptedException {
        boolean sawOnlyWildcardsSoFar = true;
        List<byte[]> reversedKeyElems = Lists.newArrayListWithCapacity(dimensions.size());

        // We build up the key in reverse order so we can leave off wildcards at the end of the key.
        // The reasoning for this is complicated, please see design docs.
        for (int i = dimensions.size() - 1; i >= 0; i--) {
            Dimension<?> dimension = dimensions.get(i);
            BucketTypeAndBucket bucketAndCoord = address.get(dimension);

            if (bucketAndCoord == null || bucketAndCoord.bucketType == BucketType.WILDCARD) {
                // We omit empty dimensions at the end of the key.
                if (sawOnlyWildcardsSoFar) {
                    continue;
                }
                byte[] e = new byte[dimension.getBucketPrefixSize() + dimension.getNumFieldBytes()];
                // add an empty byte array with enough elements to accommodate your wildcard bucket
                reversedKeyElems.add(e);
                reversedKeyElems.add(WILDCARD_FIELD);

            } else {
                sawOnlyWildcardsSoFar = false;

                byte[] bucket = getBucket(doNotCreateIds, dimension, bucketAndCoord);

                if (bucket == null) {
                    return Optional.empty();
                }


                byte[] bucketTypeId = bucketAndCoord.bucketType.getUniqueId();

                if (bucketTypeId.length != dimension.getBucketPrefixSize()) {
                    throw new RuntimeException("Bucket prefix length was wrong. For dimension " +
                            dimension + ", expected bucket prefix of length " + dimension.getBucketPrefixSize() +
                            " but the bucket prefix was " + Arrays.toString(bucketTypeId) +
                            " which had length" + bucketTypeId.length);
                }
                // add the bucket or the bucket id retrieved from the id service
                reversedKeyElems.add(bucket);
                // add the identifier for the bucket
                reversedKeyElems.add(bucketTypeId);
                // add add the indicator saying its not a wildcard
                reversedKeyElems.add(NON_WILDCARD_FIELD);
            }
        }

        // flip em
        List<byte[]> keyElemsInOrder = Lists.reverse(reversedKeyElems);

        int totalKeySize = 0;
        for (byte[] keyElement : keyElemsInOrder) {
            totalKeySize += keyElement.length;
        }

        ByteBuffer bb;
        // Add a place holder for the hash byte if it's required
        if (useAddressPrefixByteHash) {
            bb = ByteBuffer.allocate(totalKeySize + 1);
            // add a placeholder which we'll overwrite once we have an array from which we can compute the hash.
            bb.put((byte) 0x01);
        } else {
            bb = ByteBuffer.allocate(totalKeySize);
        }

        for (byte[] keyElement : keyElemsInOrder) {
            bb.put(keyElement);
        }

        byte[] array = bb.array();

        if (useAddressPrefixByteHash) {
            byte hashByte = getHashByte(array);
            bb.put(0, hashByte);
        }

        if (bb.remaining() != 0) {
            throw new AssertionError("Key length calculation was somehow wrong, " +
                    bb.remaining() + " bytes remaining");
        }

        return Optional.of(array);
    }

    private byte getHashByte(byte[] bytes) {
        return Util.hashByteArray(bytes, 1, bytes.length);
    }

    private byte[] getBucket(boolean readOnly, Dimension<?> dimension, BucketTypeAndBucket bucketAndCoord) throws IOException, InterruptedException {
        byte[] bucket;

        if (idService == null || !dimension.getDoIdSubstitution()) {
            bucket = bucketAndCoord.bucket;
        } else {
            int dimensionNum = dimensions.indexOf(dimension);
            if (readOnly) {
                final Optional<byte[]> maybeId =
                        idService.getId(dimensionNum, bucketAndCoord.bucket, dimension.getNumFieldBytes());

                bucket = maybeId.orElse(null);

                if (bucket == null) {
                    return bucket;
                }

            } else {
                bucket = idService.getOrCreateId(dimensionNum, bucketAndCoord.bucket, dimension.getNumFieldBytes());
            }
        }


        if (bucket.length != dimension.getNumFieldBytes()) {
            throw new IllegalArgumentException("Field length was wrong (after bucketing " +
                    " and unique ID substitution). For dimension " + dimension +
                    ", expected length " + dimension.getNumFieldBytes() + " but was " +
                    bucket.length);
        }
        return bucket;
    }

    @Override
    public Optional<Address> fromKey(byte[] bytes) throws IOException, InterruptedException {
        int remainingBytes = bytes.length;

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        if (useAddressPrefixByteHash) {
            // then read the first int and throw it out.
            byte hashByte = bb.get();
            remainingBytes -= Ints.BYTES;
            Preconditions.checkState(hashByte == getHashByte(bytes));
        }


        Address address = addressSupplier.apply(useAddressPrefixByteHash, dimensions);

        for (Dimension<?> dimension : dimensions) {
            if (remainingBytes <= 0) {
                // we might exit early if it turns out that we have nothing but wildcards remaining, which are omitted
                // from the serialized format.
                return Optional.of(address);
            }

            byte[] wildcardOrNah = new byte[1];
            byte[] bucketPrefix = new byte[dimension.getBucketPrefixSize()];
            byte[] bucket = new byte[dimension.getNumFieldBytes()];

            bb.get(wildcardOrNah);
            remainingBytes -= wildcardOrNah.length;

            if (Arrays.equals(wildcardOrNah, WILDCARD_FIELD)) {
                // then we need to consume dimension bytes, but do nothing else.
                bb.get(bucket);
                remainingBytes -= bucket.length;
                if (remainingBytes <= 0) {
                    // we might exit early if it turns out that we have nothing but wildcards remaining, which are omitted
                    // from the serialized format.
                    return Optional.of(address);
                }
                // nothing to add for the address for the dimension.
                continue;
            }

            bb.get(bucketPrefix);
            remainingBytes -= bucketPrefix.length;

            bb.get(bucket);
            remainingBytes -= bucket.length;

            if (dimension.getDoIdSubstitution()) {
                Optional<byte[]> maybeBucket = idService.getValueForId(dimensions.indexOf(dimension), bucket);
                if (!maybeBucket.isPresent()) {
                    return Optional.empty();
                }
                bucket = maybeBucket.get();
            }

            BucketType bucketType = null;
            for (BucketType candidate : dimension.getBucketer().getBucketTypes()) {
                if (Arrays.equals(candidate.getUniqueId(), bucketPrefix)) {
                    bucketType = candidate;
                    break;
                }
            }

            Preconditions.checkNotNull(bucketType, "could not find bucket corresponding to " + BASE_64.encode(bucketPrefix));

            address.at(dimension, bucketType, bucket);
        }
        return Optional.of(address);
    }
}
