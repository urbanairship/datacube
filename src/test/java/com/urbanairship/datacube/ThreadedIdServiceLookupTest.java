package com.urbanairship.datacube;


import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class ThreadedIdServiceLookupTest {

    @Mock
    private IdService idService;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

     @Test
     public void testUnknownKeyPositions() throws InterruptedException, IOException {
         Set<Integer> unknownKeyPositions = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
         ThreadedIdServiceLookup lookup = new ThreadedIdServiceLookup(
                 idService,
                 2,
                 "metrics"
         );

         Address address1 = Mockito.mock(Address.class);
         Address address2 = Mockito.mock(Address.class);
         Address address3 = Mockito.mock(Address.class);

         when(address1.toReadKey(idService)).thenReturn(Optional.<byte[]>absent());
         when(address2.toReadKey(idService)).thenReturn(Optional.of(new byte[]{1}));
         when(address3.toReadKey(idService)).thenReturn(Optional.<byte[]>absent());

         List<Optional<byte[]>> keys = lookup.execute(
                 ImmutableList.of(address1, address2, address3),
                 unknownKeyPositions
         );

         assertEquals(3, keys.size());
         assertFalse(keys.get(0).isPresent());
         assertTrue(keys.get(1).isPresent());
         assertFalse(keys.get(2).isPresent());

         assertArrayEquals(new byte[]{1}, keys.get(1).get());

         assertEquals(2, unknownKeyPositions.size());
         assertTrue(unknownKeyPositions.contains(0));
         assertTrue(unknownKeyPositions.contains(2));
     }

     @Test
     public void testOrderingOfResults() throws InterruptedException, IOException {
         Set<Integer> unknownKeyPositions = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
         ThreadedIdServiceLookup lookup = new ThreadedIdServiceLookup(
                 idService,
                 3,
                 "metrics"
         );

         Address address1 = Mockito.mock(Address.class);
         Address address2 = Mockito.mock(Address.class);
         Address address3 = Mockito.mock(Address.class);

         when(address1.toReadKey(idService)).thenAnswer(new Answer<Optional<byte[]>>() {
             @Override
             public Optional<byte[]> answer(InvocationOnMock invocation) throws Throwable {
                 Thread.sleep(100);
                 return Optional.of(new byte[]{0});
             }
         });

         when(address2.toReadKey(idService)).thenAnswer(new Answer<Optional<byte[]>>() {
             @Override
             public Optional<byte[]> answer(InvocationOnMock invocation) throws Throwable {
                 Thread.sleep(50);
                 return Optional.of(new byte[]{1});
             }
         });

         when(address3.toReadKey(idService)).thenReturn(Optional.of(new byte[]{2}));

         List<Optional<byte[]>> keys = lookup.execute(
                 ImmutableList.of(address1, address2, address3),
                 unknownKeyPositions
         );

         assertEquals(3, keys.size());
         assertTrue(keys.get(0).isPresent());
         assertTrue(keys.get(1).isPresent());
         assertTrue(keys.get(2).isPresent());
         assertArrayEquals(new byte[]{0}, keys.get(0).get());
         assertArrayEquals(new byte[]{1}, keys.get(1).get());
         assertArrayEquals(new byte[]{2}, keys.get(2).get());
         assertEquals(0, unknownKeyPositions.size());
     }

     @Test
     public void testFlow() throws IOException, InterruptedException {
         ThreadedIdServiceLookup lookup = new ThreadedIdServiceLookup(
                 idService,
                 2,
                 "metrics"
         );

         Address address1 = Mockito.mock(Address.class);
         Address address2 = Mockito.mock(Address.class);
         Address address3 = Mockito.mock(Address.class);
         Address address4 = Mockito.mock(Address.class);
         Address address5 = Mockito.mock(Address.class);
         Address address6 = Mockito.mock(Address.class);

         Set<Integer> unknownKeyPositions1 = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
         Set<Integer> unknownKeyPositions2 = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
         Set<Integer> unknownKeyPositions3 = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

         when(address1.toReadKey(idService)).thenThrow(new IOException("IO BOOM!"));
         when(address2.toReadKey(idService)).thenAnswer(new Answer<Optional<byte[]>>() {
             @Override
             public Optional<byte[]> answer(InvocationOnMock invocation) throws Throwable {
                 Thread.sleep(100);
                 return Optional.of(new byte[]{1});
             }
         });
         when(address3.toReadKey(idService)).thenAnswer(new Answer<Optional<byte[]>>() {
             @Override
             public Optional<byte[]> answer(InvocationOnMock invocation) throws Throwable {
                 Thread.sleep(200);
                 return Optional.absent();
             }
         });
         when(address4.toReadKey(idService)).thenReturn(Optional.of(new byte[]{3}));
         when(address5.toReadKey(idService)).thenReturn(Optional.of(new byte[]{4}));
         when(address6.toReadKey(idService)).thenReturn(Optional.of(new byte[]{5}));

         List<Optional<byte[]>> keys1 = null;

         try {
             keys1 = lookup.execute(
                     ImmutableList.of(address1, address2),
                     unknownKeyPositions1
             );

             fail();
         } catch (IOException e) {
             // this is expected
         }

         List<Optional<byte[]>> keys2 = lookup.execute(
                 ImmutableList.of(address3, address4),
                 unknownKeyPositions2
         );

         List<Optional<byte[]>> keys3 = lookup.execute(
                 ImmutableList.of(address5, address6),
                 unknownKeyPositions3
         );

         assertNull(keys1);
         assertEquals(2, keys2.size());
         assertEquals(2, keys3.size());

         assertEquals(0, unknownKeyPositions1.size());
         assertEquals(1, unknownKeyPositions2.size());
         assertEquals(0, unknownKeyPositions3.size());

         assertFalse(keys2.get(0).isPresent());
         assertTrue(keys2.get(1).isPresent());
         assertTrue(keys3.get(0).isPresent());
         assertTrue(keys3.get(1).isPresent());

         assertArrayEquals(new byte[]{3}, keys2.get(1).get());
         assertArrayEquals(new byte[]{4}, keys3.get(0).get());
         assertArrayEquals(new byte[]{5}, keys3.get(1).get());
     }

     @Test(expected = IOException.class)
     public void testReadKeyIOException() throws IOException, InterruptedException {
         Set<Integer> unknownKeyPositions = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
         ThreadedIdServiceLookup lookup = new ThreadedIdServiceLookup(
                 idService,
                 2,
                 "metrics"
         );
         Address address1 = Mockito.mock(Address.class);
         Address address2 = Mockito.mock(Address.class);

         when(address1.toReadKey(idService)).thenThrow(new IOException("IO BOOM!"));
         when(address2.toReadKey(idService)).thenReturn(Optional.of(new byte[]{1}));

         lookup.execute(ImmutableList.of(address1, address2), unknownKeyPositions);
     }

     @Test(expected = InterruptedException.class)
     public void testReadKeyInterruptedException() throws IOException, InterruptedException {
         Set<Integer> unknownKeyPositions = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
         ThreadedIdServiceLookup lookup = new ThreadedIdServiceLookup(
                 idService,
                 2,
                 "metrics"
         );
         Address address1 = Mockito.mock(Address.class);
         Address address2 = Mockito.mock(Address.class);

         when(address1.toReadKey(idService)).thenReturn(Optional.of(new byte[]{1}));
         when(address2.toReadKey(idService)).thenThrow(new InterruptedException("INTERRUPTED BOOM!"));

         lookup.execute(ImmutableList.of(address1, address2), unknownKeyPositions);
     }
}
