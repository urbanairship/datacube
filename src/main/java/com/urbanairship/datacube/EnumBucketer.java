//package com.urbanairship.datacube;
//
//import java.lang.reflect.Method;
//import java.util.List;
//import java.util.Map;
//
//import com.google.common.collect.Maps;
//
///**
// * This is untested and probably not fully baked yet -Dave
// */
//public class EnumBucketer<T extends Enum<?>> implements Bucketer {
//    private final Method getEnumValuesMethod;
//    private final Map<BucketType,Map<T,byte[]>> lookupTable = Maps.newHashMap();
//    private final Class<T> enumClass;
//    
//    public EnumBucketer(Class<T> enumClass) {
//        try {
//            getEnumValuesMethod = enumClass.getDeclaredMethod("values", (Class<?>[])null);
//        } catch (NoSuchMethodException e) {
//            throw new RuntimeException(e);
//        }
//        
//        getEnumValues(); // Fail fast. If deserialization will fail later, fail now instead.
//        
//        this.enumClass = enumClass;
//    }
//    
//    public void addMapping(BucketType bucketType, T enumVal, byte[] bucket) {
//        Map<T,byte[]> innerMap = lookupTable.get(bucketType);
//        if(innerMap == null) {
//            innerMap = Maps.newHashMap();
//            lookupTable.put(bucketType, innerMap);
//        }
//        innerMap.put(enumVal, bucket);
//    }
//
//    @Override
//    public byte[] getBucket(byte[] bucket, BucketType bucketType) {
//        Map<T,byte[]> mapForThisBucketType = lookupTable.get(bucketType);
//        if(mapForThisBucketType == null) {
//            throw new RuntimeException("Lookup table for " + enumClass + 
//                    " had no values for bucket type " + bucketType);
//        }
//        
//        if(bucket.length > 1 || bucket[0] < 0) {
//            throw new RuntimeException("EnumBucketer doesn't support enums with more than 127 instances");
//        }
//        int ordinal = bucket[0];
//        T enumInstance = getEnumValues()[ordinal];
//        byte[] bucket = mapForThisBucketType.get(enumInstance);
//        if(bucket == null) {
//            throw new RuntimeException("Lookup table for " + enumClass + " had no values for " + 
//                    "bucket type " + bucketType);
//        }
//        return bucket;
//    }
//
//    @Override
//    public List<BucketType> getBucketTypes() {
//        // TODO Auto-generated method stub
//        return null;
//    }
//    
//    private T[] getEnumValues() {
//        try {
//            return (T[])(getEnumValuesMethod.invoke(null, new Object[] {}));
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//}
