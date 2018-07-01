package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The logic of evaluation of calculate columns.
 */
class OpCalculate implements Operation {

    Column column;

    EvalCalculate lambda;
    List<ColumnPath> parameterPaths = new ArrayList<>();

    @Override
    public OperationType getOperationType() {
        return OperationType.CALCULATE;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> deps = new ArrayList<>();

        deps.add(this.column.getInput()); // Columns depend on their input table

        List<Column> cols = ColumnPath.getColumns(this.parameterPaths);
        for(Column col : cols) deps.add(col);
        return deps;
    }

    @Override
    public void evaluate() {
        if(this.lambda == null) { // Default
            this.column.setValue(); // Reset
            return;
        }

        Table mainTable = this.column.getInput(); // Loop/scan table

        //
        // Determine the scope of dirtiness
        //

        Range mainRange = mainTable.getIdRange();

        boolean fullScope = false;

        if(!fullScope) {
            if (this.column.getDefinitionChangedAt() > this.column.getChangedAt()) { // Definition has changes
                fullScope = true;
            }
        }

        if(!fullScope) { // Some column dependency has changes
            List<Element> deps = this.getDependencies();
            for(Element e : deps) {
                if(!(e instanceof Column)) continue;
                if(((Column)e).isChanged()) { // There is a column with some changes
                    fullScope = true;
                    break;
                }
            }
        }

        if(!fullScope) {
            mainRange = mainTable.getAddedRange();
        }

        //
        // Update dirty elements
        //

        // Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading valuePaths
        List<ColumnPath> paramPaths = this.parameterPaths;
        Object[] paramValues = new Object[paramPaths.size() + 1]; // Will store valuePaths for all params and current output at the end
        Object result; // Will be written to output for each input

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Read all parameter valuePaths
            for(int p=0; p<paramPaths.size(); p++) {
                paramValues[p] = paramPaths.get(p).getValue(i);
            }

            //
            // Call user-defined function
            //
            try {
                result = this.lambda.evaluate(paramValues);
            }
            catch(BistroException e) {
                throw(e);
            }
            catch(Exception e) {
                throw( new BistroException(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "Error executing user-defined function.") );
            }

            // Update output
            this.column.setValue(i, result);
        }

    }

    public OpCalculate(Column column, EvalCalculate lambda, ColumnPath[] paths) {
        this.column = column;
        this.lambda = lambda;
        this.parameterPaths = Arrays.asList(paths);
    }

    public OpCalculate(Column column, EvalCalculate lambda, Column[] columns) {
        this.column = column;
        this.lambda = lambda;
        for (int i = 0; i < columns.length; i++) {
            this.parameterPaths.add(new ColumnPath(columns[i]));
        }
    }
}
