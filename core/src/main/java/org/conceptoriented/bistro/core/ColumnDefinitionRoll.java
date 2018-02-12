package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The logic of evaluation of rolling columns.
 */
public class ColumnDefinitionRoll implements ColumnDefinition {

    Column column;

    // Dimensions for computing distance/weight
    ColumnPath[] dimensions = null;
    // Examples:
    // Empty - row id (distance between row ids)
    // One - time or 1D space. difference of values in this column
    // Two - 2D space

    // Constraint: only elements within the specified distance from the central (group) element will be accumulated
    double sizePast; // Window size (past, smaller ids)
    double sizeFuture; // Window size (future, larger ids)
    // Inclusive or exclusive? One can be inclusive and the other exclusive.

    EvaluatorRoll lambda;
    ColumnPath[] paths;

    List<BistroError> errors = new ArrayList<>();

    @Override
    public List<BistroError> getErrors() {
        return this.errors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        if(this.paths != null) {
            for(ColumnPath path : this.paths) {
                for(Column col : path.columns) {
                    if(!ret.contains(col)) ret.add(col);
                }
            }
        }

        return ret;
    }

    @Override
    public void eval() {

        errors.clear(); // Clear state

        this.column.setValue(); // Initialize to default value

        if(dimensions == null || dimensions.length == 0) {
            this.evalRows();
        }

    }

    public void evalRows() {

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
                double distance = i - min_id; // Compute distance
                if(distance <= sizePast) break; // Inside window
            }

            // Move max border forward until it does not satisfies distance (and assume exclusive)
            for( ; max_id < mainRange.end; max_id++) {
                double distance = max_id - i; // Compute distance
                if(distance > sizeFuture) break; // Outside window
            }

            //
            // For all elements in the window [min, max), prepare (aggregate,distance,params) and call roll lambda
            //

            Object aggregate = this.column.getValue(i); // Initial aggregate (will be updated)
            double distance;
            Object[] paramValues = new Object[this.paths.length];

            for(long fact_id = min_id; fact_id < max_id; fact_id++) {

                //
                // Compute distance
                //
                if(fact_id <= i) {
                    distance = i - fact_id;
                }
                else {
                    distance = fact_id - i;
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
                    aggregate = this.lambda.eval(aggregate, distance, paramValues);
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

    public ColumnDefinitionRoll(Column column, double sizePast, double sizeFuture, EvaluatorRoll lambda, ColumnPath[] paths) {
        this.column = column;

        this.sizePast = sizePast;
        this.sizeFuture = sizeFuture;

        this.lambda = lambda;
        this.paths = paths;
    }

    public ColumnDefinitionRoll(Column column, double sizePast, double sizeFuture, EvaluatorRoll lambda, Column[] columns) {
        this(column, sizePast, sizeFuture, lambda, new ColumnPath[] {});

        this.paths = new ColumnPath[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.paths[i] = new ColumnPath(columns[i]);
        }
    }

}
