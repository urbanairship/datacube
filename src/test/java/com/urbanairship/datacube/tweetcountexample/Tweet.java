package com.urbanairship.datacube.tweetcountexample;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Optional;

public class Tweet {
	private static final Pattern hashtagPattern = Pattern.compile("#(\\w+)");
	private static final Pattern retweetFromPattern = Pattern.compile("RT @(\\w+)");
//	private static final DateFormat dateFormat = 
//			new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss '+0000'");
	
	public final DateTime time;
	public final String username;
	public final Optional<String> retweetedFrom;
	public final String text;
	public final Set<String> hashTags;
	
	public Tweet(String fileLine) {
		String[] tokens = fileLine.split("\\t");
		
		time = new DateTime(Date.parse(tokens[1]), DateTimeZone.UTC);
		username = tokens[2];
		text = tokens[3];
		
		hashTags = new HashSet<String>(0);
		Matcher hashtagMatcher = hashtagPattern.matcher(text);
		while(hashtagMatcher.find()) {
			hashTags.add(hashtagMatcher.group(1));
		}

		Matcher retweetMatcher = retweetFromPattern.matcher(text);
		if(retweetMatcher.find()) {
			retweetedFrom = Optional.of(retweetMatcher.group(1));
		} else {
			retweetedFrom = Optional.absent();
		}
	}
	
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(username:" + username);
        sb.append(", time:" + time);
        sb.append(", retweeted:" + retweetedFrom);
        sb.append(", text:" + text);
        sb.append(", hashtags:" + hashTags);
        sb.append(")");
        return sb.toString();
    }
}
	

