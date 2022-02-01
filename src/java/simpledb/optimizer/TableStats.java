package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int IO_COST_PER_PAGE;
    private int tableId;
    private int fieldNum;
    private int pageNum;
    private int tupleNum;
    private DbFile dbFile;
    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StringHistogram> stringHistogramMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.IO_COST_PER_PAGE = ioCostPerPage;
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();
        this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
        this.pageNum = ((HeapFile) (dbFile)).numPages();
        TupleDesc td = this.dbFile.getTupleDesc();
        this.fieldNum = td.numFields();
        int[] mins = new int[fieldNum];
        int[] maxs = new int[fieldNum];
        Type[] types = getTypes(td);
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, tableId, "");
        try {
            scan.open();
            for (int i = 0; i < fieldNum; i ++) {
                if (types[i] == Type.STRING_TYPE) {
                    continue;
                }
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                while (scan.hasNext()) {
                    if (i == 0) {
                        tupleNum += 1;
                    }
                    Tuple tuple = scan.next();
                    int value = ((IntField) tuple.getField(i)).getValue();
                    if (value < min) {
                        min = value;
                    }
                    if (value > max) {
                        max = value;
                    }
                }
                mins[i] = min;
                maxs[i] = max;
                scan.rewind();
            }
            scan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < fieldNum; i ++) {
            if (types[i] == Type.INT_TYPE) {
                intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
            } else {
                stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        addValueToHist();

    }

    private Type[] getTypes(TupleDesc td) {
        Type[] types = new Type[td.numFields()];
        for (int i = 0, n = td.numFields(); i < n; i ++) {
            types[i] = td.getFieldType(i);
        }
        return types;
    }

    private void addValueToHist() {
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, tableId, "");
        try {
            scan.open();
            while (scan.hasNext()) {
                Tuple tuple = scan.next();
                for (int i = 0; i < fieldNum; i ++) {
                    Field field = tuple.getField(i);
                    if (field.getType() == Type.INT_TYPE) {
                        intHistogramMap.get(i).addValue(((IntField) field).getValue());
                    } else {
                        stringHistogramMap.get(i).addValue(((StringField) field).getValue());
                    }
                }
            }
            scan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return IO_COST_PER_PAGE * this.pageNum;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * this.tupleNum);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType() == Type.INT_TYPE) {
            return intHistogramMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return stringHistogramMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tupleNum;
    }

}
