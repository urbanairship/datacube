package com.urbanairship.datacube.tweetcountexample;

import java.util.List;
import java.util.Set;

import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;

public class HashTagBucketer implements Bucketer<Set<String>> {

	@Override
	public CSerializable bucketForWrite(Set<String> coordinate,
			BucketType bucketType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BucketType> getBucketTypes() {
		// TODO Auto-generated method stub
		return null;
	}

}
