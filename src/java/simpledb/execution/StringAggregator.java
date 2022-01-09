package simpledb.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private Map<Field, Integer> groupMap;
    private int gbFieldIndex;
    private Type gbFieldType;
    private int aFieldIndex;
    private Op what;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupMap = new HashMap<>();
        this.gbFieldType = gbfieldtype;
        this.gbFieldIndex = gbfield;
        this.aFieldIndex = afield;
        this.what = what;
        if (this.what != Op.COUNT) {
            throw new IllegalArgumentException("not support aggregate operate");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = (this.gbFieldIndex == NO_GROUPING ? null : tup.getField(this.gbFieldIndex));
        if (gbField != null && gbField.getType() != this.gbFieldType)
            throw new IllegalArgumentException("gb field type is not conssistency");
        groupMap.put(gbField, groupMap.getOrDefault(gbField, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregateInterator(groupMap, gbFieldType);
    }

    private class StringAggregateInterator extends AggregateIterator {

        public StringAggregateInterator(Map<Field, Integer> groupMap, Type gbFieldType) {
            super(groupMap, gbFieldType);
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return super.next();
        }
    }

}
