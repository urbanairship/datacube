package com.urbanairship.datacube;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;


/**
 * Use this class to describe a rollup that you want the datacube to keep.
 * 
 * For example, if you're counting events with the dimensions (color, size, flavor) and you
 * want to keep a total count for a (color, size) combination, you'd specify that using a Rollup. 
 */
public class Rollup {
    private final Set<DimensionAndBucketType> components;
    
    public Rollup(Set<DimensionAndBucketType> components) {
        this.components = new HashSet<DimensionAndBucketType>(components); // defensive copy
    }
    
    public Rollup(Dimension<?> d, BucketType bt) {
        this(ImmutableSet.of(new DimensionAndBucketType(d, bt)));
    }
    
    public Rollup(Dimension<?> d1, BucketType bt1, Dimension<?> d2, BucketType bt2) {
        this(ImmutableSet.of(new DimensionAndBucketType(d1, bt1),
                new DimensionAndBucketType(d2, bt2)));
    }

    public Rollup(Dimension<?> d1, BucketType bt1, Dimension<?> d2, BucketType bt2,
            Dimension<?> d3, BucketType bt3) {
        this(ImmutableSet.of(new DimensionAndBucketType(d1, bt1),
                new DimensionAndBucketType(d2, bt2), new DimensionAndBucketType(d3, bt3)));
    }
    
    public Rollup(Dimension<?> d1, Dimension<?> d2) {
        this(ImmutableSet.of(new DimensionAndBucketType(d1, BucketType.IDENTITY),
                new DimensionAndBucketType(d2, BucketType.IDENTITY)));
    }
    
    public Rollup(Dimension<?> d1, Dimension<?> d2, BucketType bt2) {
        this(ImmutableSet.of(new DimensionAndBucketType(d1, BucketType.IDENTITY),
                new DimensionAndBucketType(d2, bt2)));
    }

    Set<DimensionAndBucketType> getComponents() {
        return components;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(Rollup over ");
        sb.append(components);
        sb.append(")");
        return sb.toString();
    }
}
