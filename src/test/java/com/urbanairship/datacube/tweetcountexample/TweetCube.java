package com.urbanairship.datacube.tweetcountexample;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.urbanairship.datacube.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.bucketers.TagsBucketer;
import com.urbanairship.datacube.ops.LongOp;
/**
 * A class that wraps the datacube operations in an intuitive interface. It offers methods for counting tweets and
 * getting various counts of interest.
 */
public class TweetCube {
    private static final Logger log = LoggerFactory.getLogger(TweetCube.class);
    
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
	Dimension<Collection<String>> tagsDimension = new Dimension<Collection<String>>(
	        "tags",
	        new TagsBucketer(),
	        true,
	        7);
	List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(timeDimension, 
			retweetedFromDimension, userDimension, tagsDimension);
    /**
     * @param dbHarness storage backend implementation, see {@link DbHarness}
     * @param syncLevel how to cache and batch writes, see {@link syncLevel}
     */
    public TweetCube(DbHarness<LongOp> dbHarness, SyncLevel syncLevel) {
		/*
		 * Each rollup defines a combination of features that we'll count. E.g. count
		 * for every (user,hour) combination.
		 */
	    // Count total all-time tweets
	    Rollup allTweetsRollup = new Rollup();
		// Count all-time tweets for every user
		Rollup userRollup = new Rollup(userDimension);
		// Count tweets for each day for each user
		Rollup userHourRollup = new Rollup(userDimension, timeDimension, 
				HourDayMonthBucketer.days);
		// Count number of times each user's tweets were retweeted
		Rollup retweetedFromRollup = new Rollup(retweetedFromDimension);
		// Count number of retweets for each (originalTweeter,reTweeter) pair
		Rollup tweeterRetweeterRollup = new Rollup(userDimension, retweetedFromDimension);
		// Count hashtag occurrences
		Rollup tagRollup = new Rollup(tagsDimension);
		// Count hashtag occurrences by hour
        Rollup hourTagRollup = new Rollup(tagsDimension, timeDimension, HourDayMonthBucketer.hours);
        
		List<Rollup> rollups = ImmutableList.<Rollup>of(allTweetsRollup, userRollup, 
		        userHourRollup, retweetedFromRollup, tweeterRetweeterRollup, tagRollup,
		        hourTagRollup);
		
		/*
		 * The DataCube defines the core logic that maps input points to database
		 * increments.
		 */
		dataCube = new DataCube<LongOp>(dimensions, rollups);
		
		/*
		 * The DataCubeIo object connects the DataCube logic layer and the 
		 * DbHarness IO layer. This is the object we'll use to do reads and writes below.
		 */
		dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 0, 1000L, syncLevel, null);
	}

    /**
     * Do all the increments necessary to add a tweet to the datacube. May not immediately flush to the DB.
     */
	public void countTweet(Tweet tweet) throws IOException, InterruptedException, AsyncException {
		WriteBuilder writeBuilder = new WriteBuilder(dataCube)
			.at(timeDimension, tweet.time)
			.at(userDimension, tweet.username)
			.at(retweetedFromDimension, tweet.retweetedFrom.or(""))
		    .at(tagsDimension, tweet.hashTags);
		Batch<LongOp> cubeUpdates = dataCube.getWrites(writeBuilder, new LongOp(1));
		
        dataCubeIo.writeAsync(cubeUpdates);
	}

    /**
     * Get the total number of tweets.
     */
	public long getCount() throws InterruptedException, IOException {
	    return dataCubeIo.get(new ReadBuilder(dataCube)).or(new LongOp(0)).getLong();
	}

    /**
     * Get the total number of tweets sent by the given user.
     */
	public long getUserCount(String userName) throws InterruptedException, IOException {
		Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
			.at(userDimension, userName));
		return unpackOrZero(optCount);
	}

    /**
     * Get the number of tweets sent by the given user on the given day.
     */
	public long getUserDayCount(String userName, DateTime day) 
			throws InterruptedException, IOException {
		Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
			.at(userDimension, userName)
			.at(timeDimension, HourDayMonthBucketer.days, day));
		return unpackOrZero(optCount);
	}

    /**
     * Get the number of times the given user's tweets were retweeted.
     */
	public long getRetweetsOf(String sourceUser) throws IOException, InterruptedException {
	    Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
            .at(retweetedFromDimension, sourceUser));
	    return unpackOrZero(optCount);
	}

    /**
     * Get the number of times that retweeterUser retweeted a tweet by sourceUser.
     */
	public long getRetweetsOfBy(String sourceUser, String retweeterUser) throws IOException,
	        InterruptedException {
	    Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
	        .at(retweetedFromDimension, sourceUser)
	        .at(userDimension, retweeterUser));
	    return unpackOrZero(optCount);
	}

    /**
     * Get the number of tweets that included the given hashtag.
     */
	public long getTagCount(String hashtag) throws IOException, InterruptedException {
	    Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
            .at(tagsDimension, hashtag));
	    return unpackOrZero(optCount);
	}

    /**
     * Get the number of hashtag occurrences in the given time bucket.
     */
    protected long getTagTimeCount(String hashtag, BucketType timeBucketType, DateTime dateTime)
            throws IOException, InterruptedException {
        Optional<LongOp> optCount = dataCubeIo.get(new ReadBuilder(dataCube)
            .at(tagsDimension, hashtag)
            .at(timeDimension, timeBucketType, dateTime));
        return unpackOrZero(optCount);
    }

    /**
     * Get the number of hashtag occurrences in the given hour.
     */
    public long getTagHourCount(String hashtag, DateTime dateTime) throws IOException, InterruptedException {
        return getTagTimeCount(hashtag, HourDayMonthBucketer.hours, dateTime);
    }

    /**
     * Get the number of hashtag occurrences in the given day.
     */
    public long getTagDayCount(String hashtag, DateTime dateTime) throws IOException, InterruptedException {
        return getTagTimeCount(hashtag, HourDayMonthBucketer.days, dateTime);
    }

    /**
     * Get the number of hashtag occurrences in the given month.
     */
    public long getTagMonthCount(String hashtag, DateTime dateTime) throws IOException, InterruptedException {
        return getTagTimeCount(hashtag, HourDayMonthBucketer.months, dateTime);
    }

    /**
     * @return the value wrapped in the Optional if it is present, otherwise 0.
     */
	private static long unpackOrZero(Optional<LongOp> opt) {
	    return opt.isPresent() ? opt.get().getLong() : 0L;
	}

	public void countAll(Iterator<Tweet> tweets) throws IOException, InterruptedException, AsyncException {
	    int numCounted = 0;
		while(tweets.hasNext()) {
			countTweet(tweets.next());
			numCounted++;
			if((numCounted%1000) == 0) {
			    log.info("Counted " + numCounted);
			}
		}
	}

    /**
     * Write all batched/cached changes to the backing database.
     */
	public void flush() throws InterruptedException {
		dataCubeIo.flush();
	}
}
