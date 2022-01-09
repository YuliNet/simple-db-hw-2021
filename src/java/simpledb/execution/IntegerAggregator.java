package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupMap;
    private Map<Field, Integer> countMap;
    private Map<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
        this.countMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * 注意，当前对象只针对于一个操作，所以只需要一个map就可以实现
     * 这个对象下所表示的操作
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField afield = (IntField) tup.getField(this.afield);
        Field field = (this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield));
        if (field != null && field.getType() != this.gbfieldtype)
            throw new IllegalStateException("illegal type, please check");
        int value = afield.getValue();

        switch (this.what) {
            case MIN:
                groupMap.put(field, Math.min(groupMap.getOrDefault(field, value), value));
                break;
            case MAX:
                groupMap.put(field, Math.max(groupMap.getOrDefault(field, value), value));
                break;
            case COUNT:
                groupMap.put(field, groupMap.getOrDefault(field, 0) + 1);
                break;
            case SUM:
                groupMap.put(field, groupMap.getOrDefault(field, 0) + value);
                break;
            case SC_AVG:
                IntField countField = null;
                if (field == null)
                    countField = (IntField) tup.getField(1);
                else {
                    countField = (IntField) tup.getField(2);
                }
                int countValue = countField.getValue();
                groupMap.put(field, groupMap.getOrDefault(field, 0) + value);
                countMap.put(field, countMap.getOrDefault(field, 0) + countValue);
                break;
            case SUM_COUNT:
            case AVG:
                if (!avgMap.containsKey(field)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(value);
                    avgMap.put(field, list);
                } else {
                    List<Integer> list = avgMap.get(field);
                    list.add(value);
                    avgMap.put(field, list);
                }
                break;
            default:
                throw new IllegalArgumentException("Aggregate not supported!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggregateIterator();
    }

    private class IntAggregateIterator extends AggregateIterator {
        private Iterator<Map.Entry<Field, List<Integer>>> avgIterator;
        private boolean isAvg;
        private boolean isSumCount;
        private boolean isScAvg;

        public IntAggregateIterator() {
            super(groupMap, gbfieldtype);
            this.isAvg = what.equals(Op.AVG);
            this.isSumCount = what.equals(Op.SUM_COUNT);
            this.isScAvg = what.equals(Op.SC_AVG);
            if (isSumCount) {
                this.setTd(new TupleDesc(new Type[] {this.getGbFieldType(), Type.INT_TYPE, Type.INT_TYPE},
                                         new String[] {"groupVal", "sumVal", "countVal"}));
            }
        }

        @Override
        public void close() {
            super.close();
            if (isAvg || isSumCount) {
                avgIterator = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (isAvg || isSumCount) {
                return avgIterator.hasNext();
            }
            return super.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(this.getTd());
            if (isAvg || isSumCount) {
                Map.Entry<Field, List<Integer>> entry = avgIterator.next();
                Field f = entry.getKey();
                List<Integer> groupList = entry.getValue();
                if (isAvg) {
                    int value = groupList.stream().reduce(0, (a, b) -> a + b) / groupList.size();
                    this.setFields(tuple, value, f);
                } else {
                    int value = groupList.stream().reduce(0, (a, b) -> a + b);
                    int count = groupList.size();
                    this.setFields(tuple, value, f);
                    if (f != null) {
                        tuple.setField(2, new IntField(count));
                    } else {
                        tuple.setField(1, new IntField(count));
                    }
                }
                return tuple;
            } else if (isScAvg) {
                Map.Entry<Field, Integer> entry = this.getIterator().next();
                Field f = entry.getKey();
                int value = entry.getValue() / countMap.get(f);
                this.setFields(tuple, value, f);
                return tuple;
            }
            return super.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if (isAvg || isSumCount) {
                avgIterator = avgMap.entrySet().iterator();
            } else {
                avgIterator = null;
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if (this.isAvg || this.isSumCount) {
                this.avgIterator = avgMap.entrySet().iterator();
            }
        }
        
    }

}
