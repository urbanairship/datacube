package com.urbanairship.datacube.tweetcountexample.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.EmbeddedClusterTestAbstract;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.SyncLevel;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.tweetcountexample.TweetCube;
import com.urbanairship.datacube.tweetcountexample.TweetIterator;

/**
 * An example of counting tweets using TweetCube and reading back the counts.
 */
public class TweetCubeTests extends EmbeddedClusterTestAbstract {
	private static byte[] CUBE_TABLE = "cubeTable".getBytes();
	private static byte[] IDSERVICE_COUNTER_TABLE = "idserviceCounter".getBytes();
	private static byte[] IDSERVICE_LOOKUP_TABLE = "idserviceLookup".getBytes();
	private static byte[] CF = "c".getBytes();

    /**
     * Get a DbHarness backed by the embedded HBase test cluster.
     */
	private DbHarness<LongOp> getHBaseHarness(Configuration conf) throws Exception {
		getTestUtil().createTable(CUBE_TABLE, CF);
		getTestUtil().createTable(IDSERVICE_LOOKUP_TABLE, CF);
		getTestUtil().createTable(IDSERVICE_COUNTER_TABLE, CF);
		HTablePool hTablePool = new HTablePool(conf, Integer.MAX_VALUE);
		
		IdService idService = new HBaseIdService(conf, 
				IDSERVICE_LOOKUP_TABLE,  // The first of two tables, maps shortId->fullValue
				IDSERVICE_COUNTER_TABLE, // The second table, gives the next shortId for each dimension
				CF, 
				"".getBytes()); // The unique cube name lets multiple cubes share the same table
		
		return new HBaseDbHarness<LongOp>(hTablePool, 
				"tc".getBytes(), // Cube name, goes at the beginning of rows keys so multiple
				                 // data cubes can share the same hbase table.
				CUBE_TABLE, 
				CF, 
				new LongOp.LongOpDeserializer(), 
				idService, 
				CommitType.INCREMENT, // Use native HBase increments for mutations
				10, // Number of concurrent threads to flush mutations to the database
				2, // How many times to retry on IOException, handles temporary issues
				2, // unused unless CommiType.READ_COMBINE_CAS is used
				null); // Unique id used in metrics reporting
	}

    /**
     * The JUnit entry point for the counting example.
     */
	@Test
	public void test() throws Exception {
	    Configuration conf = getTestUtil().getConfiguration();
	    TweetCube tweetCube = new TweetCube(getHBaseHarness(conf), 
				SyncLevel.BATCH_ASYNC); // Buffer DB writes and flush when needed
		tweetCube.countAll(new TweetIterator(100));
		tweetCube.flush();
		System.out.println("Total tweets: " + tweetCube.getCount());
		System.out.println("Tweets by baraneshgh: " + tweetCube.getUserCount("baraneshgh"));
		System.out.println("Tweets retweeting IranTube: " + tweetCube.getRetweetsOf("IranTube"));
		System.out.println("Retweets of omidhabibinia by DominiqueRdr: " + 
		        tweetCube.getRetweetsOfBy("omidhabibinia", "DominiqueRdr"));
		System.out.println("Uses of hashtag #iran: " + tweetCube.getTagCount("iran"));
		DateTime hourOfInterest = new DateTime(2011, 2, 10, 15, 0, 0, 0, DateTimeZone.UTC);
		System.out.println("Uses of hashtag #iran during 2011-02-10T15:00Z: " +
                tweetCube.getTagHourCount("iran", hourOfInterest));
	}
}
