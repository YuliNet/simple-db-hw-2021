package simpledb.execution;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public class AggregateIterator implements OpIterator{

    private Iterator<Map.Entry<Field, Integer>> iterator;
    private TupleDesc td;
    private Type gbFieldType;
    private Map<Field, Integer> groupMap;

    public AggregateIterator(Map<Field, Integer> groupMap, Type gbFieldType) {
        this.groupMap = groupMap;
        this.gbFieldType = gbFieldType;
        if (gbFieldType == null) {
            this.td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[] {this.gbFieldType, Type.INT_TYPE}, new String[] {"groupVal", "aggregateVal"});
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.iterator = this.groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return iterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = iterator.next();
        Field f = entry.getKey();
        Tuple tuple = new Tuple(td);
        this.setFields(tuple, entry.getValue(), f);
        return tuple;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
       this.iterator = groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void close() {
        this.td = null;
        this.iterator = null;
    }

    protected void setFields(Tuple tuple, int value, Field f) {
        if (f == null) {
            tuple.setField(0, new IntField(value));
        } else {
            tuple.setField(0, f);
            tuple.setField(1, new IntField(value));
        }
    }

    public Iterator<Map.Entry<Field, Integer>> getIterator() {
        return iterator;
    }

    public void setIterator(Iterator<Map.Entry<Field, Integer>> iterator) {
        this.iterator = iterator;
    }

    public TupleDesc getTd() {
        return td;
    }

    public void setTd(TupleDesc td) {
        this.td = td;
    }

    public Type getGbFieldType() {
        return gbFieldType;
    }

    public void setGbFieldType(Type gbFieldType) {
        this.gbFieldType = gbFieldType;
    }

    public Map<Field, Integer> getGroupMap() {
        return groupMap;
    }

    public void setGroupMap(Map<Field, Integer> groupMap) {
        this.groupMap = groupMap;
    }
    
}
