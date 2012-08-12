package com.urbanairship.datacube.ops;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Joiner;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.Op;

public class RowOp implements IRowOp {

	private Address address;
	private final Map<BoxedByteArray, SerializableOp> columnOperations = new HashMap<BoxedByteArray, SerializableOp>();

	public RowOp(Address address) {
		this.address = address;
	}
	
	public RowOp(Address address, Map<BoxedByteArray, SerializableOp> columnOperations) {
		this(address);
		this.columnOperations.putAll(columnOperations);
	}
	
	@Override
	public Address getKey() {
		return address;
	}

	@Override
	public Map<BoxedByteArray, SerializableOp> getColumnOps() {
		return this.columnOperations;
	}

	@Override
	public RowOp add(Op otherOp) {
		if(!(otherOp instanceof RowOp)) {
			throw new IllegalArgumentException("Could not add Operations of two different types");
		}
		
		RowOp otherRowOp = (RowOp)otherOp;
		
		if(!otherRowOp.getKey().equals(getKey())) {
			throw new IllegalArgumentException("Row operations to not affect the same key");
		}
		
		RowOp newRowOp = new RowOp(this.address);
		newRowOp.addColumnOps(getColumnOps());
		
		for(Map.Entry<BoxedByteArray, SerializableOp> otherColumnOp : otherRowOp.getColumnOps().entrySet()) {
			BoxedByteArray columnQualifier = otherColumnOp.getKey();
			newRowOp.addColumnOp(columnQualifier, (SerializableOp)otherColumnOp
					.getValue().add(newRowOp.getColumnOps().get(columnQualifier)));
		}
		
		return newRowOp;
	}

	@Override
	public RowOp subtract(Op otherOp) {
		throw new NotImplementedException("Not needed so far");
	}

	@Override
	public RowOp inverse() {
		throw new NotImplementedException("Not needed so far");
	}

	@Override
	public void addColumnOp(BoxedByteArray columnQualifier, SerializableOp columnOp) {
		this.columnOperations.put(columnQualifier, columnOp);
	}

	@Override
	public void addColumnOps(Map<BoxedByteArray, SerializableOp> columnOps) {
		this.columnOperations.putAll(columnOps);
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Map.Entry<BoxedByteArray, SerializableOp> colOp : getColumnOps().entrySet()) {
			sb.append(colOp.getKey()+"="+colOp.getValue()+",");
		}
		
		return sb.toString();
	}

}
