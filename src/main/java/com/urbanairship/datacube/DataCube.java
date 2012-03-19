package com.urbanairship.datacube;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * A hypercube abstraction for storing number-like data. Good for storing counts of events
 * matching various criteria.
 * @param <T> the type of values stored in the cube. For example, a long counter could be
 * stored as a LongOp.
 */
public class DataCube<T extends Op> {
    private static final Logger log = LogManager.getLogger(DataCube.class);

    private final List<Dimension<?>> dims;
    private final List<Rollup> rollups;
    private final Multimap<Dimension<?>,BucketType> bucketsOfInterest;
    private final Set<Set<DimensionAndBucketType>> validAddressSet;

    /**
     * Use this constructor when you don't aggregate counters over multiple dimensions.
     */
    public DataCube(List<Dimension<?>> dims) {
        this(dims, null);
    }

    /**
     * @param See {@link Dimension} 
     * @param rollups {@link Rollup}
     */
    public DataCube(List<Dimension<?>> dims, List<Rollup> rollups) {
        this.dims = dims;
        this.rollups = rollups;
        this.validAddressSet = Sets.newHashSet();

        bucketsOfInterest = HashMultimap.create();

        for(Rollup rollup: rollups) {
            for(DimensionAndBucketType dimAndBucketType: rollup.getComponents()) {
                bucketsOfInterest.put(dimAndBucketType.dimension, dimAndBucketType.bucketType);
            }
            validAddressSet.add(rollup.getComponents());
        }
    }

    /**
     * Get a batch of writes that, when applied to the database, will make the change given by
     * "op".
     */
    public Batch<T> getWrites(WriteBuilder writeBuilder, T op) {
        Map<Address,T> outputMap = Maps.newHashMap(); 
        
        for(Rollup rollup: rollups) {
            Address outputAddress = new Address(this);
            
            // Start out with all dimensions wildcard, overwrite with other values later
            for(Dimension<?> dimension: dims) {
                outputAddress.at(dimension, BucketTypeAndBucket.WILDCARD);
            }
            
            for(DimensionAndBucketType dimAndBucketType: rollup.getComponents()) {
                Dimension<?> dimension = dimAndBucketType.dimension;
                BucketType bucketType = dimAndBucketType.bucketType;
                byte[] bucket = writeBuilder.getBuckets().get(dimAndBucketType);
                outputAddress.at(dimension, bucketType, bucket);
            }
            outputMap.put(outputAddress, op);
        }
        
        return new Batch<T>(outputMap);
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
        for(Entry<Dimension<?>,BucketTypeAndBucket> e: addr.getBuckets().entrySet()) {
            BucketTypeAndBucket bucketTypeAndCoord = e.getValue();
            if(bucketTypeAndCoord == BucketTypeAndBucket.WILDCARD) {
                continue;
            }
            
            Dimension<?> dimension = e.getKey();
            BucketType bucketType = bucketTypeAndCoord.bucketType;
            
            dimsAndBucketsSpecified.add(new DimensionAndBucketType(dimension, bucketType));
        }

        // Make sure that the requested address exists in this cube (is touched by some rollup)
        if(validAddressSet.contains(dimsAndBucketsSpecified)) {
            return;
        }

        // The value the user asked for isn't written by this cube
        StringBuilder errMsg = new StringBuilder();
        errMsg.append("Won't query with the dimensions given, since there are no rollups that store that ");
        errMsg.append("value. You gave dimensions: ");
        errMsg.append(dimsAndBucketsSpecified);
        errMsg.append(". To query, you must provide one of these complete sets of dimensions and buckets: (");
        boolean firstLoop = true;
        for(Rollup rollup: rollups) {
            if(!firstLoop) { 
                errMsg.append(",");
            }
            firstLoop = false;
            errMsg.append(rollup.getComponents());
        }
        errMsg.append(")");
        throw new IllegalArgumentException(errMsg.toString());
    }

    Multimap<Dimension<?>,BucketType> getBucketsOfInterest() {
        return bucketsOfInterest;
    }
}
