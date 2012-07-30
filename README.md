## Introduction

A data cube is an abstraction for counting things in complicated ways ([Wikipedia](http://en.wikipedia.org/wiki/OLAP_cube)). This project is a Java implementation of a data cube backed by a pluggable database backend.

The purpose of a data cube is to store aggregate information about large numbers of data points. The data cube stores aggregate information about interesting subsets of the input data points. For example, if you're writing a web server log analyzer, your input points could be log lines, and you might be interested in keeping a count for each browser type, each browser version, OS type, OS version, and other attributes. You might also be interested in counts for a particular *combination* of (browserType,browserVersion,osType), (browserType,browserVersion,osType,osVersion), etc. It's a challenge to quickly add and change counters without wasting time writing database code and reprocessing old data into new counters. A data cube helps you keep these counts. You declare what you want to count, and the data cube maintains all the counters as you supply new data points.

A bit more mathily, if your input data points have N attributes, then the number of counters you may have to store is the product of the cardinalities of all N attributes in the worst case. The goal of the datacube project is to help you maintain these counters in a simple declarative way without any nested switch statements or other unpleasantness.

Urban Airship uses the datacube project to support its analytics stack for mobile apps. We handle about ~10K events per second per node.

Requires JDK 1.6.

## Features
 - Performance: high-speed asynchronous batching IO backend
 - Bulk loading with Hadoop MapReduce
 - Pluggable database interface


## IO

Each input data point may affect multiple counts in the data cube. For example, if you're counting events with a timestamp, a single event may increment the count for its hour, day, month, and year, ending up with four increments that must be applied to the database. Updating the database for each of these increments wouldn't scale to thousands of events per second, so we use the standard trick of batching counter updates in the client. When an input data point is given to the data cube, it updates a batch in memory for each of the affected counters. Periodically the batches are flushed to the backing database. If a single counter is incremented multiple times in the same batch, the increments are combined into a single database update.

TODO parameters to tune, implementation details to explain parameters

## Bulk loading / backfilling


## Database backend

A data cube can be backed by any database that supports a key-value interface and allows iterating over keys. To add support for a new database backend, implement the DbHarness interface and optionally the IdService interface. See HBaseDbHarness.java and HBaseIdService.java for examples. If you add support for a new database, we'd love to have you contribute your work back into the datacube project.

Currently HBase is the only supported backing database.

## Example

```java
IdService idService = new CachingIdService(5, new MapIdService());
ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
        new ConcurrentHashMap<BoxedByteArray, byte[]>();
        
DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, 
        CommitType.READ_COMBINE_CAS, idService);

HourDayMonthBucketer hourDayMonthBucketer = new HourDayMonthBucketer();

Dimension<DateTime> time = new Dimension<DateTime>("time", hourDayMonthBucketer, false, 8);
Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), 
        true, 5);
        
DataCubeIo<LongOp> cubeIo = null;
DataCube<LongOp> cube;
        
Rollup hourAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.hours);
Rollup dayAndZipRollup = new Rollup(zipcode, time, HourDayMonthBucketer.days);
Rollup hourRollup = new Rollup(time, HourDayMonthBucketer.hours);
Rollup dayRollup = new Rollup(time, HourDayMonthBucketer.days);
        
List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(time, zipcode);
List<Rollup> rollups = ImmutableList.of(hourAndZipRollup, dayAndZipRollup, hourRollup,
        dayRollup);
        
cube = new DataCube<LongOp>(dimensions, rollups);

cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);
        
DateTime now = new DateTime(DateTimeZone.UTC);
        
// Do an increment of 5 for a certain time and zipcode
cubeIo.writeSync(new LongOp(5), new WriteBuilder(cube)
        .at(time, now)
        .at(zipcode, "97201"));
        
// Do an increment of 10 for the same zipcode in a different hour of the same day
DateTime differentHour = now.withHourOfDay((now.getHourOfDay()+1)%24);
cubeIo.writeSync(new LongOp(10), new WriteBuilder(cube)
        .at(time, differentHour)
        .at(zipcode, "97201"));

// Read back the value that we wrote for the current hour, should be 5 
Optional<LongOp> thisHourCount = cubeIo.get(new ReadBuilder(cube)
         .at(time, HourDayMonthBucketer.hours, now)
        .at(zipcode, "97201"));
Assert.assertTrue(thisHourCount.isPresent());
Assert.assertEquals(5L, thisHourCount.get().getLong());
        
// Read back the value we wrote for the other hour, should be 10
Optional<LongOp> differentHourCount = cubeIo.get(new ReadBuilder(cube)
        .at(time, HourDayMonthBucketer.hours, differentHour)
        .at(zipcode, "97201"));
Assert.assertTrue(differentHourCount.isPresent());
Assert.assertEquals(10L, differentHourCount.get().getLong());

// The total for today should be the sum of the two increments
Optional<LongOp> todayCount = cubeIo.get(new ReadBuilder(cube)
        .at(time, HourDayMonthBucketer.days, now)
        .at(zipcode, "97201"));
Assert.assertTrue(todayCount.isPresent());
Assert.assertEquals(15L, todayCount.get().getLong());
```

## Quickstart

Add datacube to your maven build. (TODO upload to a public repo)

Figure out your dimensions. These are the attributes of your incoming data points. Some examples of dimensions are time, latitude, and browser version. Create one Dimension object for each dimension. Use these dimensions to instantiate a data cube

You can skip using an IdService for now. This is an optional optimization for dimensions have that have long coordinates with low cardinality. For example, if you have a "country" dimension, the country name might be dozens of characters long, but there are only a few bytes of entropy. You could assign integers to countries and only use a few bytes to represent a country coordinate.

Create one Rollup object for each kind of counter you want to keep. For example, if you want to keep a counter of web hits by (time,browser), this would be one Rollup object.

Create a DbHarness object that will handle writing to the database. Currently, only HBaseDbHarness exists.

Create a DataCubeIo object, passing your DataCube object and your DbHarness.

Insert data points into your cube by passing them to DataCubeIo.writeSync().

Read back your rollup values by calling DataCubeIo.get().

## Building

The POM is configured to build with specific versions of HBase and Hadoop. If your versions differ from those in the POM, you can override the versions by passing hbaseVersion and hadooopVersion. For example:

```
$ mvn -DhbaseVersion=0.90.6 -DhadoopVersion=0.20.2 package
```

The build artifact jars each have a classifier of the form hbase${hbaseVersion}-hadoop${hadoopVersion}, so you can depend on them in another project by doing something like:

```
<dependency>
  <groupId>com.urbanairship</groupId>
  <artifactId>datacube</artifactId>
  <version>${datacube.version}</version>
  <classifier>hbase0.94.0-hadoop1.0.3</classifier>
</dependency>
```

The main build artifact jar (without a classifer) uses the default HBase and Hadoop versions, which may change between datacube releases.

You can pass -DhadoopVersion and -DhbaseVersion to maven to choose which version of Haodop and HBase to depend on. Hadoop 2 is not yet supported since the artifact names are different. For example: 

To build against your own version of HBase or Hadoop, just add your repository to the POM and pass -DhbaseVersion or -DhadoopVersion to the datacube build.
