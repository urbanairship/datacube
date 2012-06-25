package com.urbanairship.datacube.tweetcountexample;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.DataCubeIo;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.ReadBuilder;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.SyncLevel;
import com.urbanairship.datacube.WriteBuilder;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.ops.LongOp;

public class TweetCube {
	private final DataCubeIo<LongOp> dataCubeIo;
	private final DataCube<LongOp> dataCube;

	/*
	 *  Each dimension is some feature of a tweet.
	 */
	Dimension<DateTime> timeDimension = new Dimension<DateTime>(
			"time",                     // dimension name, for debugging output
			new HourDayMonthBucketer(), // bucketer, controls conversion to row key(s)
			false,                      // don't convert dimension values to id numbers
			8);                         // reserve 8 bytes for this field in the row key
	Dimension<String> retweetedFromDimension = new Dimension<String>(
			"retweetedFrom", 
			new StringToBytesBucketer(), 
			true, 
			4);
	Dimension<String> userDimension = new Dimension<String>(
			"user",
			new StringToBytesBucketer(), 
			true, 
			4);
	List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(timeDimension, 
			retweetedFromDimension, userDimension);

	
	public TweetCube(DbHarness<LongOp> dbHarness, SyncLevel syncLevel) {
		/*
		 * Each rollup defines a combination of features that we'll count. E.g. count
		 * for every (user,hour) combination.
		 */
		// Count all-time tweets for every user
		Rollup userRollup = new Rollup(userDimension);
		// Count tweets for each day for each user
		Rollup userHourRollup = new Rollup(userDimension, timeDimension, 
				HourDayMonthBucketer.days);
		// Count number of times each user's tweets were retweeted
		Rollup retweetedFromRollup = new Rollup(retweetedFromDimension);
		// Count number of retweets for each (originalTweeter,reTweeter) pair
		Rollup tweeterRetweeterRollup = new Rollup(userDimension, retweetedFromDimension);
//		// Count occurrences of the hashtag #IranElection
//		Rollup 
		
		List<Rollup> rollups = ImmutableList.<Rollup>of(userRollup, userHourRollup);
		
		/*
		 * The DataCube defines the core logic that maps input points to database
		 * increments.
		 */
		dataCube = new DataCube<LongOp>(dimensions, rollups);
		
		/*
		 * The DataCubeIo object connects the DataCube logic layer and the 
		 * DbHarness IO layer.
		 */
		dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 0, 1000L, syncLevel, null);
	}
	
	public void countTweet(Tweet tweet) throws IOException, InterruptedException {
		WriteBuilder writeBuilder = new WriteBuilder(dataCube)
			.at(timeDimension, tweet.time)
			.at(userDimension, tweet.username)
			.at(retweetedFromDimension, tweet.retweetedFrom.or(""));
		Batch<LongOp> cubeUpdates = dataCube.getWrites(writeBuilder, new LongOp(1));
		
		Optional<Future<?>> optFlushFuture;
		try {
			optFlushFuture = dataCubeIo.writeAsync(cubeUpdates, writeBuilder);
		} catch (Exception e) {
			// TODO more realistic exception handling
			throw new RuntimeException(e);
		}
		
		// If we got a future back, then the cube is flushing and we should wait.
		if(optFlushFuture.isPresent()) {
			try {
				optFlushFuture.get().get();
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public long getUserCount(String userName) throws InterruptedException, IOException {
		Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
			.at(userDimension, userName));
		return optCount.or(new LongOp(0)).getLong();
	}
	
	public long getUserDayCount(String userName, DateTime day) 
			throws InterruptedException, IOException {
		Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
			.at(userDimension, userName)
			.at(timeDimension, HourDayMonthBucketer.days, day));
		return optCount.or(new LongOp(0)).getLong();
	}

	public void countAll(Iterator<Tweet> tweets) throws IOException, 
			InterruptedException {
		while(tweets.hasNext()) {
			countTweet(tweets.next());
		}
	}

	public void flush() throws InterruptedException {
		dataCubeIo.flush();
	}
}
