package simpledb.optimizer;

import org.apache.commons.lang3.builder.ToStringBuilder;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int bucketSize;
    private int min;
    private int max;
    private int[] buckets;

    private int ntups;
    private double width;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.bucketSize = buckets;
        this.buckets = new int[bucketSize];
        this.min = min;
        this.max = max;
        this.width = (double) (1. + max - min) / (double) this.buckets.length;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v >= min || v < max) {
            buckets[convertValueToIndex(v)] += 1;
            this.ntups += 1;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        // EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS
        if (op == Op.LESS_THAN) {
            if (v <= min) return 0.0D;
            if (v >= max) return 1.0D;
            final int index = convertValueToIndex(v);
            double cnt = 0.0D;
            for (int i = 0 ; i < index; i ++) {
                cnt += buckets[i];
            }
            cnt += (buckets[index] / width) * (v - index * width - min);
            return (double) cnt / (double) ntups;
        } else if (op == Op.LESS_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        } else if (op == Op.GREATER_THAN) {
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        } else if (op == Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        } else if (op == Op.EQUALS) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        } else if (op == Op.NOT_EQUALS) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double cnt = 0.0D;
        for (int i = 0, n = buckets.length; i < n; i ++) {
            cnt += buckets[i];
        }
        if (cnt == 0) return 0.0D;
        return (double) (cnt / (double) ntups);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return new ToStringBuilder(this).append(bucketSize)
                                        .append(max)
                                        .append(min)
                                        .append(buckets)
                                        .append(ntups)
                                        .append(width)
                                        .toString();
    }

    private int convertValueToIndex(int v) {
        if (v < min || v > max) {
            throw new IllegalArgumentException("illegal value, please check arguments");
        }
        return (int) ((v - min) / width);
    }
}
