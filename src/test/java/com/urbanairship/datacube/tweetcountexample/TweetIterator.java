package com.urbanairship.datacube.tweetcountexample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This is an iterator that will return tweets one at a time from our test
 * data set. Each tweet is a single line.
 */
public class TweetIterator implements Iterator<Tweet> {
	public static final String TWEET_RESOURCE = "tweets_25bahman.csv";
	
	private final BufferedReader br;
	private final int stopAfter;
	
	private String nextLine;
	private int numReturned = 0;
	
	public TweetIterator() {
	    this(Integer.MAX_VALUE);
	}
	
	public TweetIterator(int stopAfter) {
	    this.stopAfter = stopAfter;
	    
	    InputStream is = getClass().getClassLoader().getResourceAsStream(TWEET_RESOURCE);
		br = new BufferedReader(new InputStreamReader(is));
		readNextInternal();
	}
	
	private void readNextInternal() {
		try {
			nextLine = br.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasNext() {
		return (nextLine != null && numReturned < stopAfter);
	}

	@Override
	public Tweet next() {
		if(nextLine == null || numReturned > stopAfter) {
			throw new NoSuchElementException();
		}
		Tweet tweetToReturn = new Tweet(nextLine);
		readNextInternal();
		numReturned++;
		return tweetToReturn;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	public static class DumpTest {
		public static void main(String[] args) {
			TweetIterator it = new TweetIterator(10);
			while(it.hasNext()) {
				System.out.println(it.next());
			}
		}
	}
}