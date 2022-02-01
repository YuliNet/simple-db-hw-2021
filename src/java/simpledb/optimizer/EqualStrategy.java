package simpledb.optimizer;

import simpledb.execution.Predicate;

public class EqualStrategy implements EstimateStrategy{

    private Predicate.Op op;
    private int v;

    public EqualStrategy(Predicate.Op op, int v) {
        this.op = op;
        this.v = v;
    }

    @Override
    public double estimate() {
        return 0;
    }
    
}
