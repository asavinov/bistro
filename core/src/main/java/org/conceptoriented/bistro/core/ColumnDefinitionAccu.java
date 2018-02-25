package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The logic of evaluation of accumulate columns.
 */
public class ColumnDefinitionAccu implements ColumnDefinition {

    Column column;

    ColumnPath groupPath;

    ColumnDefinitionCalc initDefinition;
    EvaluatorAccu lambda;
    ColumnDefinitionCalc finDefinition;

    ColumnPath[] paths;

    List<BistroError> errors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.errors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        if(this.initDefinition != null) {
            for(Element dep : this.initDefinition.getDependencies()) {
                if(!ret.contains(dep)) ret.add(dep);
            }
        }
        if(this.paths != null && this.lambda != null) {
            for(ColumnPath path : this.paths) {
                for(Column col : path.columns) {
                    if(!ret.contains(col)) ret.add(col);
                }
            }
        }
        if(this.finDefinition != null) {
            for(Element dep : this.finDefinition.getDependencies()) {
                if(!ret.contains(dep)) ret.add(dep);
            }
        }

        for(Column col : this.groupPath.columns) {
            if(!ret.contains(col)) ret.add(col);
        }

        return ret;
    }

    @Override
    public void eval() {

        //
        // Initialization
        //
        if(this.initDefinition != null) { // Default
            this.initDefinition.eval();
        }
        else {
            this.column.setValue(); // Initialize to default value
        }

        //
        // Accumulation
        //
        this.evaluateAccu();

        //
        // Finalization
        //
        if(this.finDefinition != null) { // Default
            this.finDefinition.eval();
        }
        else {
            ; // Skip finalization if not specified
        }
    }

    protected void evaluateAccu() {

        errors.clear(); // Clear state

        Table mainTable = this.groupPath.getInput(); // Loop/scan table

        Range mainRange = mainTable.getIdRange();

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

            // Evaluate
            try {
                result = this.lambda.eval(aggregate, paramValues);
            }
            catch(BistroError e) {
                this.errors.add(e);
                return;
            }
            catch(Exception e) {
                this.errors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
                return;
            }

            // Update output
            this.column.setValue(g, result);
        }
    }

    public ColumnDefinitionAccu(Column column, ColumnPath groupPath, EvaluatorAccu lambda, ColumnPath[] paths) {
        this.column = column;

        this.groupPath = groupPath;

        this.initDefinition = null;
        this.lambda = lambda;
        this.finDefinition = null;

        this.paths = paths;
    }

    public ColumnDefinitionAccu(Column column, Column groupColumn, EvaluatorAccu lambda, Column[] columns) {
        this.column = column;

        this.groupPath = new ColumnPath(groupColumn);

        this.initDefinition = null;
        this.lambda = lambda;
        this.finDefinition = null;

        this.paths = new ColumnPath[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.paths[i] = new ColumnPath(columns[i]);
        }
    }

    public ColumnDefinitionAccu(Column column, ColumnPath groupPath, EvaluatorCalc initLambda, EvaluatorAccu lambda, EvaluatorCalc finLambda, ColumnPath[] paths) {
        this.column = column;

        this.groupPath = groupPath;

        this.initDefinition = new ColumnDefinitionCalc(column, initLambda, paths);
        this.lambda = lambda;
        this.finDefinition = new ColumnDefinitionCalc(column, finLambda, paths);

        this.paths = paths;
    }

}
