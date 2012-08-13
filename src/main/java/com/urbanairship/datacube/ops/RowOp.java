package com.urbanairship.datacube.ops;

import com.google.common.base.Joiner;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.Op;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashMap;
import java.util.Map;

public class RowOp implements IRowOp {

    private Address address;
    private final Map<BoxedByteArray, SerializableOp> columnOperations = new HashMap<BoxedByteArray, SerializableOp>();
    private final static Joiner joiner = Joiner.on(',');

    public RowOp() {};

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
        checkOps(otherOp);
        RowOp otherRowOp = (RowOp) otherOp;

        RowOp newRowOp = new RowOp(this.address);
        newRowOp.addColumnOps(getColumnOps());

        for (Map.Entry<BoxedByteArray, SerializableOp> otherColumnOp : otherRowOp.getColumnOps().entrySet()) {
            BoxedByteArray columnQualifier = otherColumnOp.getKey();

            if(newRowOp.getColumnOps().containsKey(columnQualifier)) {
                newRowOp.addColumnOp(columnQualifier, (SerializableOp) otherColumnOp
                    .getValue().add(newRowOp.getColumnOps().get(columnQualifier)));
            }
        }

        return newRowOp;
    }

    /**
     * Subtracts two RowOps in the following way:
     * If an element is contained within both RowOps, then the other ops
     * value is subtracted from this ops value. Otherwise the inverse of the
     * other ops element is added to columnOps.
     */
    @Override
    public RowOp subtract(Op otherOp) {
        checkOps(otherOp);
        RowOp otherRowOp = (RowOp) otherOp;

        RowOp newRowOp = new RowOp(this.address);
        newRowOp.addColumnOps(getColumnOps());

        for (Map.Entry<BoxedByteArray, SerializableOp> otherColumnOp : otherRowOp.getColumnOps().entrySet()) {
            BoxedByteArray columnQualifier = otherColumnOp.getKey();

            if (newRowOp.getColumnOps().containsKey(columnQualifier)) {
                newRowOp.addColumnOp(columnQualifier,
                    (SerializableOp) newRowOp.getColumnOps().get(columnQualifier).subtract(otherColumnOp.getValue()));
            } else {
                newRowOp.addColumnOp(columnQualifier, (SerializableOp) otherColumnOp.getValue().inverse());
            }
        }

        return newRowOp;
    }

    private void checkOps(Op otherOp) {
        if (!(otherOp instanceof IRowOp)) {
            throw new IllegalArgumentException("Could not add Operations of two different types");
        }

        IRowOp otherRowOp = (IRowOp)otherOp;
        if ((getKey() != null && otherRowOp.getKey() != null)
            && getKey() != otherRowOp.getKey()
            && !otherRowOp.getKey().equals(getKey())) {
            throw new IllegalArgumentException("Row operations to not affect the same key");
        }
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
        StringBuilder sb = new StringBuilder();
        joiner.join(getColumnOps().entrySet());
        for (Map.Entry<BoxedByteArray, SerializableOp> colOp : getColumnOps().entrySet()) {
            sb.append(colOp.getKey() + "=" + colOp.getValue() + ",");
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime
            * result
            + ((columnOperations == null) ? 0 : columnOperations.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowOp other = (RowOp) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        if (columnOperations == null) {
            if (other.columnOperations != null)
                return false;
        } else if (!columnOperations.equals(other.columnOperations))
            return false;
        return true;
    }


}
