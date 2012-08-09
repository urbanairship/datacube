/**
 * Copyright (C) 2012 Neofonie GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.urbanairship.datacube;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Set;

/**
 * This class describes operations on the datacube.
 * A slice is an operation during which the cube is reduced
 * by one or more dimensions (with 3 dimensions a slice looks like a slice),
 * e.g. all cities counts for year "2004" and language "english".
 *
 * The idea is to use a special "SLICE" value to identify a slice
 * for a dimension (acts like a wildcard).
 *
 * This would result in a key schema which would contain the SLICE flag
 * and the values/buckets of every other dimension;
 *
 * (color, size, taste)
 *
 * (SLICE, big, bad) => {red: 1, yellow: 12, green: 23}
 *
 * The slices would be executed during writes, similar to rollups
 * and naturally lead to more writes but an increased read-performance.
 *
 * For now all dimensions and their values have to be specified (no wildcard support for now)
 */
public class Slice {

    private ImmutableSet<DimensionAndBucketType> dimsAndBuckets;
    private static final String SLICE_WILDCARD_VALUE = "SLICE_";
    private Dimension<?> variableDim;
    private static final byte[] SLICE_WILDCARD_VALUE_BYTES = Bytes.toBytes(Slice.SLICE_WILDCARD_VALUE);

    /**
     * Constructs a Slice operation based on one dimension
     * @param sliceDimension the dimension to base the slice on
     * @param dimsAndBuckets dimensions for whose buckets values should be preaggregated
     */
    public Slice(Dimension<?> sliceDimension, Set<DimensionAndBucketType> dimsAndBuckets) {
    	this.dimsAndBuckets = ImmutableSet.copyOf(dimsAndBuckets);
        this.variableDim = sliceDimension;
    }

	public Dimension<?> getVariableDimension() {
        return variableDim;
	}

	protected static String getWildcardValue() {
        return Slice.SLICE_WILDCARD_VALUE;
	}

    protected static byte[] getWildcardValueBytes() {
        return Slice.SLICE_WILDCARD_VALUE_BYTES;
    }

	public ImmutableSet<DimensionAndBucketType> getDimensionsAndBucketTypes() {
		return this.dimsAndBuckets;
	}

}
