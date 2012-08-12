/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.dbharnesses;

import com.google.common.base.Optional;
import com.urbanairship.datacube.*;
import com.urbanairship.datacube.ops.IRowOp;
import com.urbanairship.datacube.ops.SerializableOp;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * For testing, this is is a backing store for a cube that lives in memory. It saves us from
 * calling a DB just to test the cube logic.
 */
public class MapDbHarness<T extends SerializableOp> implements DbHarness<T> {
    private final static Logger log = LogManager.getLogger(MapDbHarness.class);

    private static final int casRetries = 10;
    private static final Future<?> nullFuture = new NullFuture();

    private final ConcurrentMap<BoxedByteArray,byte[]> map;
    private final Deserializer<T> deserializer;
    private final CommitType commitType;
    private final IdService idService;

    public MapDbHarness(ConcurrentMap<BoxedByteArray,byte[]> map, Deserializer<T> deserializer,
            CommitType commitType, IdService idService) {
        this.map = map;
        this.deserializer = deserializer;
        this.commitType = commitType;
        this.idService = idService;
        if(commitType != CommitType.OVERWRITE && commitType != CommitType.READ_COMBINE_CAS) {
            throw new IllegalArgumentException("MapDbHarness doesn't support commit type " +
                    commitType);
        }
    }

    /**
     * Actually synchronous and not asyncronous, which is allowed.
     */
    @Override
    public Future<?> runBatchAsync(Batch<T> batch, AfterExecute<T> afterExecute) {

        for(Map.Entry<Address,IRowOp> entry: batch.getMap().entrySet()) {
            Address address = entry.getKey();
            IRowOp opFromBatch = entry.getValue();

            BoxedByteArray mapKey;
            try {
                mapKey = new BoxedByteArray(address.toKey(idService));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if(commitType == CommitType.READ_COMBINE_CAS) {
					try {
						readCombineCasColumn(afterExecute, address, opFromBatch, mapKey);
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
						throw new RuntimeException("Unable to deserialize column-map, because class is unknown", e);
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException("Failed deserializing object due to unknown reasons", e);
					}
	                //`readCombineCas(afterExecute, address, opFromBatch, mapKey);
            } else if(commitType == CommitType.OVERWRITE) {
            	Map<BoxedByteArray, BoxedByteArray> columnMap = new HashMap<BoxedByteArray, BoxedByteArray>();
            	
            	for(Map.Entry<BoxedByteArray, SerializableOp> colOp : opFromBatch.getColumnOps().entrySet()) {
	                BoxedByteArray serOp = new BoxedByteArray(colOp.getValue().serialize());
            		columnMap.put(colOp.getKey(), serOp);
            	}
            	
                try {
                    map.put(mapKey, serializeMap(columnMap));
                } catch (IOException e) {
                    throw new RuntimeException("Was unable to serialize column map", e);
                }

                if(log.isDebugEnabled()) {
                    log.debug("Write of key " + Hex.encodeHexString(mapKey.bytes));
                }

            } else {
                throw new AssertionError("Unsupported commit type: " + commitType);
            }
        }
        batch.reset();
        afterExecute.afterExecute(null); // null throwable => success
        return nullFuture;
    }

    private void readCombineCasColumn(AfterExecute<T> afterExecute, Address address, IRowOp opFromBatch, BoxedByteArray mapKey) throws IOException, ClassNotFoundException {

        int casRetriesRemaining = casRetries;

    	do {
	    	// check if key is avaliable
	    	Optional<byte[]> currentValueOptional = Optional.absent();
	    	byte[] currentValue = null;

	    	try {
				currentValueOptional = getRaw(address);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

	    	boolean success = false;

	    	if(currentValueOptional.isPresent()) {
	    		currentValue = currentValueOptional.get();
		    	Map<BoxedByteArray, BoxedByteArray> columnMap = deserializeMap(currentValue);
		    	
		    	for(Map.Entry<BoxedByteArray, SerializableOp> colOp : opFromBatch.getColumnOps().entrySet()) {
			    	SerializableOp combinedOp;
	
			    	if(columnMap.containsKey(colOp.getKey())) {
				    	SerializableOp currentColumnOp = deserializer.fromBytes(columnMap.get(colOp.getKey()).bytes);
				    	combinedOp = (SerializableOp) colOp.getValue().add(currentColumnOp);
			    	} else {
			    		combinedOp = colOp.getValue();
			    	}
			    	
			    	columnMap.put(colOp.getKey(), new BoxedByteArray(combinedOp.serialize()));
		    	}

		    	success = map.replace(mapKey, currentValue, serializeMap(columnMap));
	    	} else {
	    		Map<BoxedByteArray, BoxedByteArray> newColumnMap = new ConcurrentHashMap<BoxedByteArray, BoxedByteArray>();
                for(Map.Entry<BoxedByteArray, SerializableOp> colOp : opFromBatch.getColumnOps().entrySet()) {
                	BoxedByteArray columnKey = colOp.getKey();
                	BoxedByteArray value = new BoxedByteArray(colOp.getValue().serialize());
		    		newColumnMap.put(columnKey, value);
                }
	    		if(map.putIfAbsent(mapKey, serializeMap(newColumnMap)) == null) {
	    			success = true;
	    		}
	    	}

	    	if(success) {
	    		break;
	    	}

    	} while(casRetriesRemaining-- > 0);


        if(casRetriesRemaining == -1) {
            RuntimeException e = new RuntimeException("CAS retries exhausted");
            afterExecute.afterExecute(e);
            throw e;
        }

    }

    private <K,V> Map<K,V> deserializeMap(byte[] serializedMap) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(serializedMap);
        ObjectInput oin = null;
        oin = new ObjectInputStream(bis);

        @SuppressWarnings("unchecked")
        Map<K, V> map = (Map<K, V>) oin.readObject();
        return map;
    }

    private <K,V> byte[] serializeMap(Map<K, V> map) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(map);
        oos.close();

        return bos.toByteArray();
    }

    @Override
    public Optional<T> get(Address address) throws IOException, InterruptedException {
        Optional<byte[]> bytes = getRaw(address);
        if(bytes.isPresent()) {
	        try {
				Map<BoxedByteArray, BoxedByteArray> columnMap = deserializeMap(bytes.get());
				// TODO: reference to datacube does not belong here
				BoxedByteArray emptyColumnBytes = columnMap.get(DataCube.EMPTY_COLUMN_QUALIFIER);
				
				if(emptyColumnBytes == null) {
					return Optional.absent();
				}
				// TODO: empty column constant
	            return Optional.of(deserializer.fromBytes(emptyColumnBytes.bytes));
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new RuntimeException("Unable to deserialize column map", e);
			}
        } else {
            return Optional.absent();
        }
    }


    @Override
    public Optional<Map<BoxedByteArray, T>> getSlice(Address sliceAddr) {
        Map<BoxedByteArray, BoxedByteArray> columnMap;
        Map<BoxedByteArray, T> resultMap = new HashMap<BoxedByteArray, T>();

        try {
            Optional<byte[]> result = getRaw(sliceAddr);

            if(!result.isPresent()) {
            	return Optional.absent();
            }

            columnMap = deserializeMap(result.get());

            for(Map.Entry<BoxedByteArray, BoxedByteArray> entry : columnMap.entrySet()) {
                T deserializedOp = this.deserializer.fromBytes(entry.getValue().bytes);
                resultMap.put(entry.getKey(), deserializedOp);
            }

            return Optional.of(resultMap);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Optional.absent();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Optional.absent();
    }

    @Override
    public void flush() throws InterruptedException {
        return; // all ops are synchronously applied, nothing to do
    }

    private Optional<byte[]> getRaw(Address address) throws InterruptedException {
        byte[] mapKey;
        try {
            mapKey = address.toKey(idService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        byte[] bytes = map.get(new BoxedByteArray(mapKey));
        if(log.isDebugEnabled()) {
            log.debug("getRaw for key " + Hex.encodeHexString(mapKey) + " returned " +
                    Arrays.toString(bytes));
        }
        if(bytes == null) {
            return Optional.absent();
        } else {
            return Optional.of(bytes);
        }
    }

    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        throw new NotImplementedException();
    }

    private static class NullFuture implements Future<Object> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                TimeoutException {
            return null;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }
}
