package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The logic of evaluation of rolling columns.
 */
class OpRoll implements Operation {

    Column column;

    // Dimensions for computing distance/weight
    ColumnPath distancePath = null;
    // Examples:
    // Empty - row id (distance between row ids)
    // One - time or 1D space. difference of values in this column
    // Two - 2D space
    // etc.

    // It is essentially a comparator so maybe check the corresponding standard interface
    private double computeDistance(long pastId, long futureId) {
        if(distancePath == null) {
            return (double)(futureId - pastId);
        }
        else {
            return ((Number)this.distancePath.getValue(futureId)).doubleValue() - ((Number)this.distancePath.getValue(pastId)).doubleValue();
        }
    }

    // Constraint: only elements within the specified distance from the central (group) element will be accumulated
    double sizePast; // Window size (past, smaller ids, minimum) - exclusive
    double sizeFuture; // Window size (future, larger ids, maximum) - inclusive

    EvalRoll lambda;
    ColumnPath[] paths;

    @Override
    public OperationType getOperationType() {
        return OperationType.ROLL;
    }

    List<BistroError> errors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.errors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> deps = new ArrayList<>();

        deps.add(this.column.getInput()); // Columns depend on their input table

        if(this.distancePath != null) {
            for(Column col : this.distancePath.columns) {
                if(!deps.contains(col)) deps.add(col);
            }
        }

        if(this.paths != null) {
            for(ColumnPath path : this.paths) {
                for(Column col : path.columns) {
                    if(!deps.contains(col)) deps.add(col);
                }
            }
        }

        return deps;
    }

    @Override
    public void evaluate() {

        errors.clear(); // Clear state

        this.column.setValue(); // Initialize to default value

        Table mainTable = this.column.getInput(); // Loop/scan table

        Range mainRange = mainTable.getIdRange();

        long min_id = mainRange.start;
        long max_id = mainRange.start;

        for(long i=mainRange.start; i<mainRange.end; i++) {

            //
            // Update window by moving its borders [min,max) forward
            //

            // Move min border forward until it satisfies distance (assume inclusive)
            for( ; min_id <= i; min_id++) {
                if(computeDistance(min_id, i) < sizePast) break; // Inside window
            }

            // Move max border forward until it does not satisfies distance (and assume exclusive)
            for( ; max_id < mainRange.end; max_id++) {
                if(computeDistance(i, max_id) > sizeFuture) break; // Outside window
            }

            //
            // For all elements in the window [min, max), prepare (aggregate,distance,params) and call roll adder
            //

            Object aggregate = this.column.getValue(i); // Initial aggregate (will be updated)
            double distance;
            Object[] paramValues = new Object[this.paths.length];

            for(long fact_id = min_id; fact_id < max_id; fact_id++) {

                //
                // Compute distance
                //
                if(fact_id <= i) {
                    distance = computeDistance(fact_id, i);
                }
                else {
                    distance = computeDistance(i, fact_id);
                }

                //
                // Read parameters
                //
                for(int p=0; p<this.paths.length; p++) {
                    paramValues[p] = this.paths[p].getValue(fact_id);
                }

                //
                // Evaluate
                //
                try {
                    aggregate = this.lambda.evaluate(aggregate, distance, paramValues);
                }
                catch(BistroError e) {
                    this.errors.add(e);
                    return;
                }
                catch(Exception e) {
                    this.errors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
                    return;
                }

            }

            this.column.setValue(i, aggregate); // Store final aggregate in the column
        }

    }

    public OpRoll(Column column, ColumnPath distancePath, double sizePast, double sizeFuture, EvalRoll lambda, ColumnPath[] paths) {
        this.column = column;

        this.distancePath = distancePath;

        this.sizePast = sizePast;
        this.sizeFuture = sizeFuture;

        this.lambda = lambda;
        this.paths = paths;
    }

    public OpRoll(Column column, Column distanceColumn, double sizePast, double sizeFuture, EvalRoll lambda, Column[] columns) {
        this(column, distanceColumn != null ? new ColumnPath(distanceColumn) : null, sizePast, sizeFuture, lambda, new ColumnPath[] {});

        this.paths = new ColumnPath[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.paths[i] = new ColumnPath(columns[i]);
        }
    }

}
