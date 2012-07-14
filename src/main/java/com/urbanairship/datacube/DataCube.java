/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
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
    private static final Logger log = LoggerFactory.getLogger(DataCube.class);

    private final List<Dimension<?>> dims;
    private final List<Rollup> rollups;
    private final Multimap<Dimension<?>,BucketType> bucketsOfInterest;
    private final Set<Set<DimensionAndBucketType>> validAddressSet;
    private final Map<Rollup,RollupFilter> filters = Maps.newHashMap();

    /**
     * @param dims see {@link Dimension} 
     * @param rollups see {@link Rollup}
     */
    public DataCube(List<Dimension<?>> dims, List<Rollup> rollups) {
        this.dims = dims;
        this.rollups = rollups;
        this.validAddressSet = Sets.newHashSet();

        bucketsOfInterest = HashMultimap.create();

        if(dims.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("May not have more than " + Short.MAX_VALUE + 
                    " dimensions");
        }
        
        for(Rollup rollup: rollups) {
            for(DimensionAndBucketType dimAndBucketType: rollup.getComponents()) {
                if(!dims.contains(dimAndBucketType.dimension)) {
                    throw new IllegalArgumentException("Rollup dimension " + 
                            dimAndBucketType.dimension + " is not a dimension in this cube");
                }
                
                if(!dimAndBucketType.dimension.getBucketer().getBucketTypes().contains(dimAndBucketType.bucketType)) {
                    throw new IllegalArgumentException("Rollup specified bucket type " + 
                            dimAndBucketType.bucketType + " which doesn't exist for dimension " +
                            dimAndBucketType.dimension);
                }
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
            
            
            boolean shouldWrite = true;
            
            RollupFilter rollupFilter = filters.get(rollup); 
            if(rollupFilter != null) {
                Object attachment = writeBuilder.getRollupFilterAttachments().get(rollupFilter);
                Optional<Object> attachmentOpt = Optional.fromNullable(attachment);
                shouldWrite = rollupFilter.filter(outputAddress, attachmentOpt);
            }
            
            if(shouldWrite) {
                outputMap.put(outputAddress, op);
            }
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
    
    public void addFilter(Rollup rollup, RollupFilter filter) {
        filters.put(rollup, filter);
    }
    
}
