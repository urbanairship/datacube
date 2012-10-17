package com.urbanairship.datacube.tweetcountexample.tests;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.urbanairship.datacube.tweetcountexample.Tweet;

public class TweetParseTest {
	@Test
	public void testParsing() {
		String inputLine = "35720274552823808	Thu, 10 Feb 2011 15:22:31 +0000	iran88	RT @TehranBureau: As #Karroubi placed under house arrest after #25Bahman rally call, lead adviser detained. http://to.pbs.org/fsWae4 #Iran #IranElection";
		Tweet tweet = new Tweet(inputLine);
		Assert.assertEquals("iran88", tweet.username);
		Assert.assertEquals(Optional.of("TehranBureau"), tweet.retweetedFrom);
		Assert.assertEquals(new DateTime(2011, 2, 10, 15, 22, 31, 0, DateTimeZone.UTC), tweet.time);
		Assert.assertEquals(Sets.newHashSet("Karroubi", "25Bahman", "Iran", "IranElection"), 
				tweet.hashTags);
	}
}
