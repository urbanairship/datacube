/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

/**
 * This enum is used to configure the flushing behavior of a {@link DataCubeIo}.
 * 
 * FULL_SYNC: every write to the cube is flushed immediate to the underlying DbHarness in a way that
 * will cause it to write to the DB immediately. When the DataCubeIo write returns, the op will be
 * persisted in the DB.
 * 
 * Pro:<ul>
 *  <li>Multiple writer threads do not block each other</li>
 * </ul>
 * Con:<ul>
 *  <li>Each write is slow because it touches the DB.</li>
 * </ul>
 * 
 * BATCH_SYNC: writes are not immediately flushed to the database but stored in memory in batches.
 * When the DataCubeIo detects that it's time to flush its batch, or when {@link DataCubeIo#flush}
 * is called, the batch will be written to the DB, blocking the writing thread.
 * 
 * Pro:<ul>
 *  <li>Better efficiency than FULL_SYNC by combining multiple writes in batches in memory.</li>
 *  <li>You don't have to worry about asynchronous exceptions. Any exceptions that occur will be thrown
 *    at the time that you write.</li>
 * </ul>
 * Con:<ul>
 *  - The writing thread will block until the flush completes, instead of possibly doing useful work
 * </ul>
 * 
 * BATCH_ASYNC: writes are not immediately flushed to the database but stored in memory in batches.
 * When the DataCubeIo detects that it's time to flush its batch, or when {@link DataCubeIo#flush}
 * is called, the batch will be enqueued aynchronously for writing to the DB.
 * 
 * Pro:<ul>
 *   <li>RFastest. The database worker threads will be kept busy flushing the queue of batches, which will
 *     lead to the higest throughput.</li>
 *   <li>Negligible blocking of writers.</li>
 * </ul>
 * 
 * Con:<ul>
 *   <li>A synchronous flush is impossible. You have no way to force all operations in memory to go to
 *     the DB. The best you can do is put them in the async queue.</li>
 *   <li>Failure handling is complicated. When you put a batch on the async queue and return, the batch
 *     may fail later and the original caller will not be informed unless it calls Future.get().</li>
 * </ul>
 */
public enum SyncLevel {FULL_SYNC, BATCH_SYNC, BATCH_ASYNC}
