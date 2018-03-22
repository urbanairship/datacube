/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A hypercube abstraction for storing number-like data. Good for storing counts of events
 * matching various criteria.
 *
 * @param <T> the type of values stored in the cube. For example, a long counter could be
 *            stored as a LongOp.
 */
public class DataCube<T extends Op> {
    private static final Logger log = LoggerFactory.getLogger(DataCube.class);

    public enum PREFIX_MODE {NO_ADDRESS_PREFIX, MOD_ADDRESS_PREFIX}

    private final List<Dimension<?>> dims;
    private final List<Rollup> rollups;
    private final Multimap<Dimension<?>, BucketType> bucketsOfInterest;
    private final Map<Set<DimensionAndBucketType>, Rollup> validAddressSet;
    private final boolean useAddressPrefixByteHash;

    /**
     * @param dims    see {@link Dimension}
     * @param rollups see {@link Rollup}
     */
    public DataCube(List<Dimension<?>> dims, List<Rollup> rollups) {
        this(dims, rollups, PREFIX_MODE.NO_ADDRESS_PREFIX);
    }

    /**
     * @param dims       see {@link Dimension}
     * @param rollups    see {@link Rollup}
     * @param prefixMode use MOD_ADDRESS_PREFIX to prefix the keys by a hash byte (calculated by hashing each element
     *                   in the key).  This is only a storage artifact to benefit systems like HBase, where
     *                   monotonically increasing row keys can result in hot spots.
     *
     *                   Warning: Do NOT switch modes for an existing cube or the keys will
     *                   not map properly.  Also, data from versions of datacube before 2.0.0,
     *                   with this feature enabled, is not compatible with 2.0.0+.
     */
    public DataCube(List<Dimension<?>> dims, List<Rollup> rollups, PREFIX_MODE prefixMode) {
        this.dims = dims;
        this.rollups = rollups;
        this.validAddressSet = Maps.newHashMap();
        if (PREFIX_MODE.MOD_ADDRESS_PREFIX == prefixMode) {
            this.useAddressPrefixByteHash = true;
        } else {
            this.useAddressPrefixByteHash = false;
        }

        bucketsOfInterest = HashMultimap.create();

        if (dims.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("May not have more than " + Short.MAX_VALUE +
                    " dimensions");
        }

        for (Rollup rollup : rollups) {
            for (DimensionAndBucketType dimAndBucketType : rollup.getComponents()) {
                if (!dims.contains(dimAndBucketType.dimension)) {
                    throw new IllegalArgumentException("Rollup dimension " +
                            dimAndBucketType.dimension + " is not a dimension in this cube");
                }

                bucketsOfInterest.put(dimAndBucketType.dimension, dimAndBucketType.bucketType);
            }
            validAddressSet.put(new HashSet<>(rollup.getComponents()), rollup);
        }
    }

    /**
     * Get a batch of writes that, when applied to the database, will make the change given by
     * "op".
     */
    public Batch<T> getWrites(WriteBuilder writeBuilder, T op) {
        Map<Address, T> outputMap = Maps.newHashMap();

        for (Rollup rollup : rollups) {

            List<Set<byte[]>> coordSets =
                    new ArrayList<Set<byte[]>>(rollup.getComponents().size());

            boolean dimensionHadNoBucket = false;
            for (DimensionAndBucketType dimAndBucketType : rollup.getComponents()) {
                Dimension<?> dimension = dimAndBucketType.dimension;
                BucketType bucketType = dimAndBucketType.bucketType;

                SetMultimap<BucketType, byte[]> bucketsForDimension =
                        writeBuilder.getBuckets().get(dimension);

                if (bucketsForDimension == null || bucketsForDimension.isEmpty()) {
                    dimensionHadNoBucket = true;
                    break;
                }

                Set<byte[]> coords = bucketsForDimension.get(bucketType);
                if (coords == null || coords.isEmpty()) {
                    dimensionHadNoBucket = true;
                    break;
                }
                coordSets.add(coords);
            }

            if (dimensionHadNoBucket) {
                // Skip this rollup since one of its input buckets was not present.
                log.debug("Skipping rollup due to dimension with no buckets");
                continue;
            }

            Set<List<byte[]>> crossProduct = Sets.cartesianProduct(coordSets);

            for (List<byte[]> crossProductTuple : crossProduct) {
                Address outputAddress = new Address(this, rollup);

                // Start out with all dimensions wildcard, overwrite with other values later
                for (Dimension<?> dimension : dims) {
                    outputAddress.at(dimension, BucketTypeAndBucket.WILDCARD);
                }

                for (int i = 0; i < crossProductTuple.size(); i++) {
                    byte[] coord = crossProductTuple.get(i);
                    Dimension<?> dim = rollup.getComponents().get(i).dimension;
                    BucketType bucketType = rollup.getComponents().get(i).bucketType;

                    outputAddress.at(dim, new BucketTypeAndBucket(bucketType, coord));
                }

                boolean shouldWrite = true;

                if (shouldWrite) {
                    outputMap.put(outputAddress, op);
                }
            }
        }

        return new Batch<T>(outputMap);
    }

    public boolean useAddressPrefixByteHash() {
        return useAddressPrefixByteHash;
    }

    public List<Dimension<?>> getDimensions() {
        return dims;
    }

    /**
     * @throws IllegalArgumentException if addr isn't in the cube.
     */
    void checkValidReadOrThrow(Address addr) {
        Set<DimensionAndBucketType> dimsAndBucketsSpecified = new HashSet<DimensionAndBucketType>(addr.getBuckets().size());

        // Find out which dimensions have literal values (not wildcards)
        for (Entry<Dimension<?>, BucketTypeAndBucket> e : addr.getBuckets().entrySet()) {
            BucketTypeAndBucket bucketTypeAndCoord = e.getValue();
            if (bucketTypeAndCoord == BucketTypeAndBucket.WILDCARD) {
                continue;
            }

            Dimension<?> dimension = e.getKey();
            BucketType bucketType = bucketTypeAndCoord.bucketType;

            dimsAndBucketsSpecified.add(new DimensionAndBucketType(dimension, bucketType));
        }

        // Make sure that the requested address exists in this cube (is touched by some rollup)
        final Rollup sourceRollup = validAddressSet.get(dimsAndBucketsSpecified);
        if (sourceRollup != null) {
            addr.setSourceRollup(sourceRollup);
            return;
        }

        // The value the user asked for isn't written by this cube
        StringBuilder errMsg = new StringBuilder();
        errMsg.append("Won't query with the dimensions given, since there are no rollups that store that ");
        errMsg.append("value. You gave dimensions: ");
        errMsg.append(dimsAndBucketsSpecified);
        errMsg.append(". To query, you must provide one of these complete sets of dimensions and buckets: (");
        boolean firstLoop = true;
        for (Rollup rollup : rollups) {
            if (!firstLoop) {
                errMsg.append(",");
            }
            firstLoop = false;
            errMsg.append(rollup.getComponents());
        }
        errMsg.append(")");
        throw new IllegalArgumentException(errMsg.toString());
    }

    Multimap<Dimension<?>, BucketType> getBucketsOfInterest() {
        return bucketsOfInterest;
    }
}
