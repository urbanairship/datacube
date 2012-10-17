/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

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
                
                bucketsOfInterest.put(dimAndBucketType.dimension, dimAndBucketType.bucketType);
            }
            validAddressSet.add(new HashSet<DimensionAndBucketType>(rollup.getComponents()));
        }
    }

    /**
     * Get a batch of writes that, when applied to the database, will make the change given by
     * "op".
     */
    public Batch<T> getWrites(WriteBuilder writeBuilder, T op) {
        Map<Address,T> outputMap = Maps.newHashMap(); 
        
        for(Rollup rollup: rollups) {
            
            List<Set<byte[]>> coordSets = 
                    new ArrayList<Set<byte[]>>(rollup.getComponents().size());

            boolean dimensionHadNoBucket = false;
            for(DimensionAndBucketType dimAndBucketType: rollup.getComponents()) {
                Dimension<?> dimension = dimAndBucketType.dimension;
                BucketType bucketType = dimAndBucketType.bucketType;

                SetMultimap<BucketType,byte[]> bucketsForDimension =
                        writeBuilder.getBuckets().get(dimension);

                if(bucketsForDimension == null || bucketsForDimension.isEmpty()) {
                    dimensionHadNoBucket = true;
                    break;
                }

                Set<byte[]> coords = bucketsForDimension.get(bucketType);
                if(coords == null || coords.isEmpty()) {
                    dimensionHadNoBucket = true;
                    break;
                }
                coordSets.add(coords);
            }

            if(dimensionHadNoBucket) {
                // Skip this rollup since one of its input buckets was not present.
                log.debug("Skipping rollup due to dimension with no buckets");
                continue;
            }
            
            Set<List<byte[]>> crossProduct = Sets.cartesianProduct(coordSets);
            
            for(List<byte[]> crossProductTuple: crossProduct) {
                Address outputAddress = new Address(this);
                
                // Start out with all dimensions wildcard, overwrite with other values later
                for(Dimension<?> dimension: dims) {
                    outputAddress.at(dimension, BucketTypeAndBucket.WILDCARD);
                }
                
                for(int i=0; i<crossProductTuple.size(); i++) {
                    byte[] coord = crossProductTuple.get(i);
                    Dimension<?> dim = rollup.getComponents().get(i).dimension;
                    BucketType bucketType = rollup.getComponents().get(i).bucketType;
                    
                    outputAddress.at(dim, new BucketTypeAndBucket(bucketType, coord));
                }
                
                boolean shouldWrite = true;
                
                // If there is a RollupFilter for this Rollup, it may prevent this write
                // from proceeding for application-specific reasons.
                RollupFilter rollupFilter = filters.get(rollup);
                if(rollupFilter != null) {
                    Optional<Object> attachment = Optional.fromNullable(
                            writeBuilder.getRollupFilterAttachments().get(rollupFilter));
                    shouldWrite = rollupFilter.filter(outputAddress, attachment);
                }
                
                if(shouldWrite) {
                    outputMap.put(outputAddress, op);
                }
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
