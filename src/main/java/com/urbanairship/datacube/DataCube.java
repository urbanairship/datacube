package com.urbanairship.datacube;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DataCube<T extends Op> {
//    private final String name;
    private final List<Dimension> dims;
    private final Optional<List<Aggregate>> aggregates;
    
    /**
     * Use this constructor when you don't aggregate counters over multiple dimensions.
     */
    public DataCube(List<Dimension> dims) {
        this.dims = dims;
        this.aggregates = Optional.absent();
    }
    
    public DataCube(String name, List<Dimension> dims, List<Aggregate> aggregates) {
        this.dims = dims;
        this.aggregates = Optional.of(aggregates);
    }
    
    public Batch<T> getWrites(UnexplodedAddress address, T op) {
        int numDimensions = dims.size();
        
        // Find the cross product of all bucketed values ... brace yourself
        List<List<BucketTypeAndCoord>> explodedCoords = Lists.newArrayList();
        for(int i=0; i<numDimensions; i++) {
            // Get all the buckets to write to in this single dimension
            Dimension dimension = dims.get(i);
            byte[] inputCoordThisDimension = address.get(dimension);
            
            List<BucketTypeAndCoord> bucketsThisDimension = Lists.newArrayList();
            Bucketer bucketer = dimension.getBucketer();
            for(BucketType bucketType: bucketer.getBucketTypes()) {
                byte[] bucket = bucketer.getBucket(inputCoordThisDimension, bucketType);
                bucketsThisDimension.add(new BucketTypeAndCoord(bucketType, bucket));
            }
            explodedCoords.add(bucketsThisDimension);
        }
        
        System.err.println("Bucket values: " + explodedCoords);
        
        int[] bucketCursors = new int[numDimensions];
        
        List<ExplodedAddress> explodedAddresses = Lists.newArrayList();
        
        while(true) {
            ExplodedAddress explodedAddress = new ExplodedAddress();
            boolean finished = false;
            for(int dimIdx=0; dimIdx<numDimensions; dimIdx++) {
                int cursorThisDim = bucketCursors[dimIdx];
                explodedAddress.at(dims.get(dimIdx), explodedCoords.get(dimIdx).get(cursorThisDim));
                
            }
            explodedAddresses.add(explodedAddress);
        
            // Advance cursors to the next tuple in the cross product
            for(int i=0; i<bucketCursors.length; i++) {
                bucketCursors[i] += 1;
                if(bucketCursors[i] == explodedCoords.get(i).size()) {
                    if(i == numDimensions-1) {
                        finished = true;
                        break;
                    }
                    bucketCursors[i] = 0;
                } else {
                    break;
                }
            }
            if(finished) {
                break;
            }
        }

//        for(Dimension dimension: dims) {
//            Bucketer bucketer = dimension.getBucketer();
//            if(bucketer != null) {
//                /* This dimension has a bucketer. Each cell we were planning to write to will be turned
//                 * into multiple cells, one for each bucket returned by the bucketer.
//                 * 
//                 * For example, suppose we're counting events by hour, day, and month. When we get
//                 * the event with the coordinates (2012-3-8-12:30:05, purple), this needs to be 
//                 * exploded into the three buckets (hourly_march_8_2012_12oclock, purple), 
//                 * (daily_march_8_2012, purple) and (monthly_march_2012, purple).
//                 */
//                List<Address> newExplodedAddresses = new ArrayList<Address>();
//                for(Address oldAddress: explodedAddresses) {
//                    byte[] coord = oldAddress.get(dimension);
//                    for(Entry<BucketType,byte[]> e: bucketer.getBucketForCoord(coord).entrySet()) {
//                        BucketType bucketType = e.getKey();
//                        byte[] newCoord = e.getValue();
//                        // This single Address is turning into multiple exploded Address's, one for
//                        // each bucket.
//                        System.err.println("Unbucketed val " + Arrays.toString(coord) +
//                                " turned into bucket " + Arrays.toString(newCoord));
//                        
//                        // The new coordinates in this dimension are the BucketType id followed
//                        // by the bucketed coord
//                        Address addressAfterExplosion = new Address(oldAddress);
//                        addressAfterExplosion.atCoord(dimension, ArrayUtils.addAll(
//                                bucketType.getUniqueId(), newCoord));
//                        
//                        newExplodedAddresses.add(addressAfterExplosion);
//                    }
//                }
//                explodedAddresses = newExplodedAddresses;
//            }
//        }
        
        System.out.println("Complete exploded addresses: " + explodedAddresses);
        
        // TODO Bucketize into potentially multiple buckets
        
        // TODO add aggregate cells
        
        if(aggregates.isPresent()) {
            throw new NotImplementedException();
        }
        
        Map<ExplodedAddress,T> newBatchMap = Maps.newHashMap();
        for(ExplodedAddress explodedAddress: explodedAddresses) {
            newBatchMap.put(explodedAddress, op);
        }
        return new Batch<T>(newBatchMap);
    }
    
    public List<Dimension> getDimensions() {
        return dims;
    }
    
    public ExplodedAddress bucketize(ExplodedAddress addr) {
        ExplodedAddress bucketed = new ExplodedAddress();
        for(Dimension dimension: dims) {
            BucketTypeAndCoord beforeBucketing = addr.get(dimension);
            if(beforeBucketing == null) {
                throw new IllegalArgumentException("Address had no coordinate for dimension " + 
                        dimension);
            }
            BucketType bucketType = beforeBucketing.bucketType;
            bucketed.at(dimension, bucketType, 
                    dimension.getBucketer().getBucket(beforeBucketing.coord, bucketType));
        }
        return bucketed;
    }
    
//    private static boolean advance(int[] cursors) {
//        
//    }
}
