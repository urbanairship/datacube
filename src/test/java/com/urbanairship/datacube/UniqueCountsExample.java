package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;

/**
 * An example of counting unique users over multiple time intervals. 
 */
public class UniqueCountsExample {
    static class Event {
        private final DateTime time;
        private final String username;
        
        public Event(DateTime time, String username) {
            this.time = time;
            this.username = username;
        }
    }
    
    static Dimension<DateTime> timeDimension = new Dimension<DateTime>("time",
            new HourDayMonthBucketer(), false, 8);
    
    // For a given hour/day/month, which users have we already seen?
    static Multimap<DateTime,String> seenUsersByHour = HashMultimap.create();
    static Multimap<DateTime,String> seenUsersByDay = HashMultimap.create();
    static Multimap<DateTime,String> seenUsersByMonth = HashMultimap.create();
    
    /**
     * This is a RollupFilter that will decline to count any events if the username was already
     * seen for the given time interval.
     */
    static RollupFilter uniqueCountFilter = new RollupFilter() {
        @Override
        public boolean filter(Address address, Optional<Object> attachment) {
            Assert.assertTrue(attachment.isPresent());
            String username = (String)attachment.get();
            
            byte[] timestampBytes = address.get(timeDimension).bucket;
            DateTime dateTime = new DateTime(Util.bytesToLong(timestampBytes), DateTimeZone.UTC);
             
            Multimap<DateTime,String> checkUniquenessMap;
            BucketType bucketType = address.get(timeDimension).bucketType; 
            if(bucketType == HourDayMonthBucketer.hours) {
                checkUniquenessMap = seenUsersByHour;
            } else if(bucketType == HourDayMonthBucketer.days) {
                checkUniquenessMap = seenUsersByDay;
            } else if(bucketType == HourDayMonthBucketer.months) {
                checkUniquenessMap = seenUsersByMonth;
            } else {
                throw new AssertionError("Unexpected bucket type");
            }
            
            boolean firstTimeSeen = checkUniquenessMap.put(dateTime, username);
            return firstTimeSeen;
        }
    };
    
    /**
     * A wrapper class that hides cube manipulation behind easy-to-read function names.
     */
    private static class CubeWrapper {
        private DataCube<LongOp> dataCube;
        private DataCubeIo<LongOp> dataCubeIo;
        
        public CubeWrapper() {
            
            Rollup hourRollup = new Rollup(timeDimension, HourDayMonthBucketer.hours);
            Rollup dayRollup = new Rollup(timeDimension, HourDayMonthBucketer.days);
            Rollup monthRollup = new Rollup(timeDimension, HourDayMonthBucketer.months);
            List<Rollup> rollups = ImmutableList.of(hourRollup, dayRollup, monthRollup);
            
            List<Dimension<?>> dims = ImmutableList.<Dimension<?>>of(timeDimension);
            
            dataCube = new DataCube<LongOp>(dims, rollups);
            IdService idService = new MapIdService();
            ConcurrentMap<BoxedByteArray,byte[]> backingMap = Maps.newConcurrentMap();
            DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap,LongOp.DESERIALIZER, 
                    CommitType.READ_COMBINE_CAS, idService);
            this.dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 1, Long.MAX_VALUE,
                    SyncLevel.FULL_SYNC);
            
            dataCube.addFilter(hourRollup, uniqueCountFilter);
            dataCube.addFilter(dayRollup, uniqueCountFilter);
            dataCube.addFilter(monthRollup, uniqueCountFilter);
        }
        
        public void put(Event event) throws IOException, InterruptedException {
            WriteBuilder writeBuilder = new WriteBuilder(dataCube)
                .at(timeDimension, event.time)
                .attachForRollupFilter(uniqueCountFilter, event.username);
            dataCubeIo.writeSync(new LongOp(1), writeBuilder);
        }
        
        public long getUniqueUsersForHour(DateTime timestamp) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(dataCube)
                .at(timeDimension, HourDayMonthBucketer.hours, timestamp));
            if(countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }

        public long getUniqueUsersForDay(DateTime timestamp) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(dataCube)
                .at(timeDimension, HourDayMonthBucketer.days, timestamp));
            if(countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }
        public long getUniqueUsersForMonth(DateTime timestamp) throws IOException, InterruptedException {
            Optional<LongOp> countOpt = dataCubeIo.get(new ReadBuilder(dataCube)
                .at(timeDimension, HourDayMonthBucketer.months, timestamp));
            if(countOpt.isPresent()) {
                return countOpt.get().getLong();
            } else {
                return 0L;
            }
        }
    }
    
    @Test
    public void test() throws Exception {
        DateTime monthBegin = new DateTime(DateTimeZone.UTC).withMillisOfDay(0).withDayOfMonth(1);
        List<Event> events = ImmutableList.of(
                new Event(monthBegin.plusHours(1).plusMinutes(1), "bob"),
                new Event(monthBegin.plusHours(1).plusMinutes(2), "bob"),
                new Event(monthBegin.plusHours(1).plusMinutes(3), "frank"),
                new Event(monthBegin.plusHours(5).plusMinutes(2), "bob"),
                new Event(monthBegin.minusHours(1).plusMinutes(10), "bob"),
                new Event(monthBegin.minusHours(1).plusMinutes(5), "frank"),
                new Event(monthBegin.minusHours(1).plusMinutes(30), "frank"));
        
        CubeWrapper cubeWrapper = new CubeWrapper();
        
        for(Event event: events) {
            cubeWrapper.put(event);
        }
        
        Assert.assertEquals(2L, cubeWrapper.getUniqueUsersForHour(monthBegin.plusHours(1)));
        Assert.assertEquals(1L, cubeWrapper.getUniqueUsersForHour(monthBegin.plusHours(5)));
        Assert.assertEquals(2L, cubeWrapper.getUniqueUsersForHour(monthBegin.minusHours(1)));
        
        Assert.assertEquals(2L, cubeWrapper.getUniqueUsersForDay(monthBegin));
        Assert.assertEquals(2L, cubeWrapper.getUniqueUsersForDay(monthBegin.minusDays(1)));
        
        Assert.assertEquals(2L, cubeWrapper.getUniqueUsersForMonth(monthBegin));
        Assert.assertEquals(2L, cubeWrapper.getUniqueUsersForMonth(monthBegin.minusMonths(1)));
    }
}
