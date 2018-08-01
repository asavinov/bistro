package org.conceptoriented.bistro.core.operations;

import java.util.ArrayList;
import java.util.List;

import org.conceptoriented.bistro.core.*;

/**
 * The logic of evaluation of accumulate columns.
 */
public class OpAccumulate implements Operation {

    Column column;

    ColumnPath groupPath;

    EvalAccumulate adder;
    EvalAccumulate remover;

    ColumnPath[] paths;

    @Override
    public OperationType getOperationType() {
        return OperationType.ACCUMULATE;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> deps = new ArrayList<>();

        deps.add(this.column.getInput()); // Columns depend on their input table

        deps.add(this.groupPath.getInput()); // Accumulate column depends on the fact table

        for(Column col : this.groupPath.columns) {
            if(!deps.contains(col)) deps.add(col);
        }

        // Updater dependencies
        if(this.paths != null && (this.adder != null || this.remover != null)) {
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

        //
        // Initialize new elements
        //
        this.evalInitialier();

        //
        // Update group elements by data from added/removed facts
        //
        Table mainTable = this.groupPath.getInput(); // Loop/scan table - fact table (not the table product this column is defined)

        if(this.remover != null) {
            Range removedRange = mainTable.getRemovedRange();
            this.evalUpdater(removedRange, this.remover);
        }
        if(this.adder != null) {
            Range addedRange = mainTable.getAddedRange();
            this.evalUpdater(addedRange, this.adder);
        }

        // Full re-evaluation
        //Range mainRange = mainTable.getIdRange();
        //this.evalUpdater(mainRange, this.adder);
    }

    protected void evalInitialier() {
        Table mainTable = this.column.getInput();
        if(mainTable.isChanged()) {
            Range addedRange = mainTable.getAddedRange();
            this.column.setValue(addedRange);
        }
    }

    protected void evalUpdater(Range mainRange, EvalAccumulate lambda) {

        // Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading valuePaths
        Object[] paramValues = new Object[this.paths.length]; // Will store valuePaths for all params
        Object result; // Will be written to output for each input
        Object aggregate;

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Find group, that is, projection of the current fact to the group table
            Object g_out = this.groupPath.getValue(i);
            if(g_out == null) {
                continue; // Do not accumulate facts without group
            }
            Long g = (Long)g_out;
            if(g == -1) {
                continue; // Do not accumulate facts without group
            }

            // Read all parameter valuePaths
            for(int p=0; p<this.paths.length; p++) {
                paramValues[p] = this.paths[p].getValue(i);
            }

            // Read current out value
            aggregate = this.column.getValue(g);

            //
            // Call user-defined function
            //
            try {
                result = lambda.evaluate(aggregate, paramValues);
            }
            catch(BistroException e) {
                throw(e);
            }
            catch(Exception e) {
                throw(new BistroException(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "Error executing user-defined function."));
            }

            // Update output
            this.column.setValue(g, result);
        }
    }

    public OpAccumulate(Column column, ColumnPath groupPath, EvalAccumulate adder, EvalAccumulate remover, ColumnPath[] paths) {
        this.column = column;

        this.groupPath = groupPath;

        this.adder = adder;
        this.remover = remover;

        this.paths = paths;
    }

    public OpAccumulate(Column column, Column groupColumn, EvalAccumulate adder, EvalAccumulate remover, Column[] columns) {
        this.column = column;

        this.groupPath = new ColumnPath(groupColumn);

        this.adder = adder;
        this.remover = remover;

        this.paths = new ColumnPath[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.paths[i] = new ColumnPath(columns[i]);
        }
    }
}
