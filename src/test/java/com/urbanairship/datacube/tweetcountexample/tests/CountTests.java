package com.urbanairship.datacube.tweetcountexample.tests;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.Test;

import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.SyncLevel;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.tweetcountexample.TweetCube;
import com.urbanairship.datacube.tweetcountexample.TweetIterator;

public class CountTests {
	private static byte[] CUBE_TABLE = "cubeTable".getBytes();
	private static byte[] IDSERVICE_COUNTER_TABLE = "idserviceCounter".getBytes();
	private static byte[] IDSERVICE_LOOKUP_TABLE = "idserviceLookup".getBytes();
	private static byte[] CF = "c".getBytes();

	private DbHarness<LongOp> getHBaseHarness() throws Exception {
		HBaseTestingUtility testUtil = new HBaseTestingUtility();
		testUtil.startMiniCluster();
		testUtil.createTable(CUBE_TABLE, CF);
		testUtil.createTable(IDSERVICE_LOOKUP_TABLE, CF);
		testUtil.createTable(IDSERVICE_COUNTER_TABLE, CF);
		HTablePool hTablePool = new HTablePool(testUtil.getConfiguration(), Integer.MAX_VALUE);
		
		IdService idService = new HBaseIdService(testUtil.getConfiguration(), 
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
				5, // Number of concurrent threads to flush mutations to the databse
				2, // How many times to retry on IOException, handles temporary issues
				2, // unused unless CommiType.READ_COMBINE_CAS is used
				null); // Unique id used in metrics reporting
	}
	
	@Test
	public void test() throws Exception {
		TweetCube tweetCube = new TweetCube(getHBaseHarness(), 
				SyncLevel.BATCH_ASYNC); // Buffer DB writes and flush when needed
		tweetCube.countAll(new TweetIterator());
		tweetCube.flush();
		System.out.println("Tweets by baraneshgh: " + tweetCube.getUserCount("baraneshgh"));
		System.out.println("Tweets retweeting IranTube: " + tweetCube.getUserCount("baraneshgh"));
		
	}
}
