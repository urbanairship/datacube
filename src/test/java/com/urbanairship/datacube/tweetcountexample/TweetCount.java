//package com.urbanairship.datacube.tweetcountexample;
//
//import org.apache.commons.lang.ArrayUtils;
//import org.junit.Test;
//
///**
// * This is an example of how you might count things using a datacube.
// * 
// * We'll count tweets matching certain criteria. TODO write more
// */
//public class TweetCount {
//	@Test
//	public void test() {
//		TweetIterator it = new TweetIterator();
//		
//		while(it.hasNext()) {
//			Tweet tweet = it.next();
//			
//			String[] tokens = tweet.split("\\t");
//			
//			
//			System.out.println(ArrayUtils.toString(tokens));
//		}
//	}
//}
