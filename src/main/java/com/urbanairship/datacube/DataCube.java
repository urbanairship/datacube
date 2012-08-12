/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.urbanairship.datacube.ops.IRowOp;
import com.urbanairship.datacube.ops.RowOp;
import com.urbanairship.datacube.ops.SerializableOp;

import java.util.*;
import java.util.Map.Entry;

/**
 * A hypercube abstraction for storing number-like data. Good for storing counts of events
 * matching various criteria.
 * @param <T> the type of values stored in the cube. For example, a long counter could be
 * stored as a LongOp.
 */
public class DataCube<T extends SerializableOp> {
	public static final BoxedByteArray EMPTY_COLUMN_QUALIFIER = new BoxedByteArray("".getBytes());

    private final List<Dimension<?>> dims;
    private final List<Rollup> rollups;
    private final Multimap<Dimension<?>,BucketType> bucketsOfInterest;
    private final Set<Set<DimensionAndBucketType>> validAddressSet;
    private final Map<Rollup,RollupFilter> filters = Maps.newHashMap();
    // TODO: final, replace temp constructor
    private List<Slice> slices = Collections.emptyList();

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

        // TODO: temporary constructor
    public DataCube(List<Dimension<?>> dimensions, List<Rollup> rollups, ImmutableList<Slice> slices) {
        this(dimensions, rollups);
        this.slices = slices;
    }

    /**
     * Get a batch of writes that, when applied to the database, will make the change given by
     * "op".
     */
    public Batch<T> getWrites(WriteBuilder writeBuilder, SerializableOp op) {
        Map<Address,IRowOp> outputMap = Maps.newHashMap();

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
            	IRowOp singleOpRowOp = new RowOp(outputAddress);
            	singleOpRowOp.addColumnOp(EMPTY_COLUMN_QUALIFIER, op);
                outputMap.put(outputAddress, singleOpRowOp);
            }
        }

        // derive writes based on the slices registered with this cube
        for(Slice slice : this.slices) {
            Map<DimensionAndBucketType,byte[]> buckets = writeBuilder.getBuckets();
            // every slice writes only one address, based on the
            // "variable" and "fixed" dimensions provided
            // the operation performed on the column-family is
            // a ColumnOp<T extends Op>, which holds an Op for a
            // key/column name
            Address sliceOutAddr = new Address(this);

            Dimension<?> sliceDimension = slice.getVariableDimension();
            sliceOutAddr.at(sliceDimension, Slice.getWildcardValueBytes());
            Set<DimensionAndBucketType> dims = slice.getDimensionsAndBucketTypes();

            for(DimensionAndBucketType dimbucket : dims) {
            	Dimension<?> dim = dimbucket.dimension;
            	BucketType btype = dimbucket.bucketType;
            	byte[] value = buckets.get(dimbucket);

                sliceOutAddr.at(dim, btype, value);
            }

            byte[] columnKey = null;

            for(Map.Entry<DimensionAndBucketType, byte[]> dimAndBtype : buckets.entrySet()) {
            	if(dimAndBtype.getKey().dimension.equals(sliceDimension)) {
            		columnKey = dimAndBtype.getValue();
            		break;
            	}
            }

            if(columnKey == null) {
            	throw new RuntimeException("unable to find column key");
            }

            IRowOp sliceRowOp = new RowOp(sliceOutAddr);
            sliceRowOp.addColumnOp(new BoxedByteArray(columnKey), op);
            outputMap.put(sliceOutAddr, sliceRowOp);
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
