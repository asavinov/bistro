package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The logic of evaluation of accumulate columns.
 */
public class ColumnDefinitionAccu implements ColumnDefinition {

    Column column;

    ColumnDefinitionCalc initDefinition;
    Expression accuExpr;
    ColumnDefinitionCalc finDefinition;

    ColumnPath groupPath;

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
        if(this.accuExpr != null) {
            for(Column col : ColumnPath.getColumns(this.accuExpr.getParameterPaths())) {
                if(!ret.contains(col)) ret.add(col);
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
        if(this.initDefinition == null) { // Default
            this.column.setValue(); // Initialize to default value
        }
        else {
            this.initDefinition.eval();
        }

        //
        // Accumulation
        //
        this.evaluateExpr();

        //
        // Finalization
        //
        if(this.finDefinition == null) { // Default
            ; // Skip finalization if not specified
        }
        else {
            this.finDefinition.eval();
        }
    }

    protected void evaluateExpr() {

        errors.clear(); // Clear state

        Table mainTable = this.groupPath.getInput(); // Loop/scan table

        Range mainRange = mainTable.getIdRange();

        // Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading valuePaths
        List<ColumnPath> paramPaths = this.accuExpr.getParameterPaths();
        Object[] paramValues = new Object[paramPaths.size() + 1]; // Will store valuePaths for all params and current output at the end
        Object result; // Will be written to output for each input

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
            for(int p=0; p<paramPaths.size(); p++) {
                paramValues[p] = paramPaths.get(p).getValue(i);
            }

            // Read current out value and store as the last element of parameter array
            paramValues[paramValues.length-1] = this.column.getValue(g); // [ACCU-specific] [FIN-specific]

            // Evaluate
            try {
                result = this.accuExpr.eval(paramValues);
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

    public ColumnDefinitionAccu(Column column, ColumnPath groupPath, Expression accuExpr) {
        this.column = column;

        this.groupPath = groupPath;

        this.initDefinition = null;
        this.accuExpr = accuExpr;
        this.finDefinition = null;
    }
    public ColumnDefinitionAccu(Column column, ColumnPath groupPath, Expression initExpr, Expression accuExpr, Expression finExpr) {
        this.column = column;

        this.groupPath = groupPath;

        this.initDefinition = new ColumnDefinitionCalc(column, initExpr);
        this.accuExpr = accuExpr;
        this.finDefinition = new ColumnDefinitionCalc(column, finExpr);
    }

}
