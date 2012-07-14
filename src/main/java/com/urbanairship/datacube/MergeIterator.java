/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ListMultimap;

/**
 * An iterator that merges multiple input iterators into a single sorted iterator. 
 * 
 * The input iterators must yield their items in sorted order. It's implemented as a heap where each 
 * iterator has one item on the heap.
 * 
 * Not thread safe.
 * 
 * This whole class was copied verbatim from Shennendoah. 
 */
public class MergeIterator<T> implements Iterator<ListMultimap<Iterator<T>,T>>{
    private static final Logger log = LoggerFactory.getLogger(MergeIterator.class);
    
    private final List<Iterator<T>> iterators;
    private final PriorityQueue<HeapEntry> heap;
    private final Map<Iterator<T>,String> debugLabels = new HashMap<Iterator<T>,String>();
    
    private NavigableSet<TraceEntry> trace = null;
    
    /**
     * @param iterators An array of iterators, where each iterator's output is already sorted in the order 
     * defined by comparator.
     */
    public MergeIterator(final Comparator<T> comparator, List<Iterator<T>> iterators) {
        this(comparator, iterators, false);
    }

    /**
     * @param iterators An array of iterators, where each iterator's output is already sorted in the order 
     * defined by comparator.
     * @param trace whether or not to maintain a history of iterator invocations and print it
     * if there's a RuntimeException
     */
    public MergeIterator(final Comparator<T> comparator, List<Iterator<T>> iterators, boolean trace) {
        this.iterators = iterators;
        this.heap = new PriorityQueue<HeapEntry>(iterators.size(), new Comparator<HeapEntry>() {
            @Override
            public int compare(HeapEntry o1, HeapEntry o2) {
                return comparator.compare(o1.item, o2.item);
            }
        });

        if(trace) {
            this.trace = new TreeSet<TraceEntry>();
        }

        int i=0;
        for (Iterator<T> iterator: iterators) {
            if (hasNextWrapper(iterator)) {
                heap.add(new HeapEntry(nextWrapper(iterator), i));
            }
            i++;
        }
    }
    
    /**
     * Get the next group of results that compare equal.
     * 
     * @return a ListMultimap containing the next batch of results that compare equal. The values are
     * grouped together by the iterator they came from. So the map keys are iterators, and the map values
     * are items that came from that iterator. 
     */
    @Override
    public ListMultimap<Iterator<T>,T> next() {
        ListMultimap<Iterator<T>,T> results = ArrayListMultimap.create();
        while (true) {
            HeapEntry heapEntry = heap.poll();
            if (heapEntry == null) { 
                break; // heap is empty
            }
            results.put(iterators.get(heapEntry.fromIterator), heapEntry.item); 

            // The heap must contain one entry for each iterator that has >= 1 item remaining. 
            // Replace the one we just popped with a new one from the source iterator.
            Iterator<T> sourceIterator = iterators.get(heapEntry.fromIterator);
            if (hasNextWrapper(sourceIterator)) {
                HeapEntry newEntry = new HeapEntry(nextWrapper(sourceIterator), heapEntry.fromIterator);
                
                // Any new heap items should compare >= popped items if input iterators are sorted
                assert heap.comparator().compare(newEntry, heapEntry) >= 0;
                
                heap.add(newEntry);
            }
            
            // Iterate again if and only if the next item on the heap should be returned with this one
            // (that is, they compare equal by the given comparator).
            HeapEntry peekHeap = heap.peek();
            if (peekHeap == null || heap.comparator().compare(heapEntry, heap.peek()) != 0) {
                break;
            }
        }
        
        if (results.size() == 0) {
            throw new NoSuchElementException(); // Input iterators are empty
        }
        return results;
    }
    
    public void setDebugLabel(Iterator<T> iterator, String label) {
        debugLabels.put(iterator, label);
    }
    
    /**
     * This is the type of the objects that go on the heap of pending items. Its only purpose is to
     * store an item along with the index of the input iterator that it came from.
     */
    private class HeapEntry {
        public T item;
        public int fromIterator;
        
        public HeapEntry(T item, int fromIterator) {
            this.item = item;
            this.fromIterator = fromIterator;
        }
    }

    @Override
    public boolean hasNext() {
        return !heap.isEmpty();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Get and return the next value from an iterator, and add a tracing record if enabled.
     * Logs tracing info if a RuntimeException occurs.
     */
    private T nextWrapper(Iterator<T> it) {
        try {
            if(trace != null) {
                trace.add(new TraceEntry(System.nanoTime(), System.currentTimeMillis(), 
                        NextOrHasNext.NEXT, debugLabels.get(it)));
            }
            return it.next();
        } catch (RuntimeException e) {
            logTrace();
            throw e;
        }
    }
    
    private boolean hasNextWrapper(Iterator<T> it) {
        try {
            if(trace != null) {
                trace.add(new TraceEntry(System.nanoTime(), System.currentTimeMillis(), 
                        NextOrHasNext.HASNEXT, debugLabels.get(it)));
            }
            return it.hasNext();
        } catch (RuntimeException e) {
            logTrace();
            throw e;
        }           
    }
    
    private void logTrace() {
        StringBuilder errMsg = new StringBuilder();
        DateTimeFormatter fmt = ISODateTimeFormat.basicDateTime();
        if(trace != null) {
            errMsg.append("Trace: \n");
            for(TraceEntry traceEntry : trace) {
                String ts = fmt.print(new DateTime(traceEntry.timeMs, DateTimeZone.UTC));
                errMsg.append(ts + " -> " + traceEntry.nextOrHasNext + ":" + traceEntry.label + "\n");
            }
        } else {
            errMsg.append("Tracing not enabled.");
        }
        log.error(errMsg.toString());
    }
    
    private enum NextOrHasNext {NEXT, HASNEXT}
    
    private class TraceEntry implements Comparable<TraceEntry> {
        public final long nanoTicks;
        public final long timeMs;
        public final NextOrHasNext nextOrHasNext;
        public final String label;
        
        public TraceEntry(long nanoTicks, long timeMs, NextOrHasNext nextOrHasNext, String label) {
            this.nanoTicks = nanoTicks;
            this.timeMs = timeMs;
            this.nextOrHasNext = nextOrHasNext;
            this.label = label;
        }

        @Override
        public int compareTo(TraceEntry other) {
            long nanoDelta = this.nanoTicks - other.nanoTicks;
            if(nanoDelta < 0) {
                return -1;
            } else if (nanoDelta > 0) {
                return 1;
            } else {
                // Nanosecond times were equal, fallback to ordering based on other fields.
                return ComparisonChain.start().compare(timeMs, other.timeMs).compare(label, other.label)
                        .compare(nextOrHasNext, other.nextOrHasNext).result();
            }
             
        }
    }
}
