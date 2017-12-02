package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The logic of accumulate columns.
 */
public class ColumnDefinitionAccu implements ColumnDefinition {

    Column column;

    ColumnDefinitionCalc initDefinition;
    Expression accuExpr;
    ColumnDefinitionCalc finDefinition;

    ColumnPath accuPath;

    List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
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

        for(Column col : this.accuPath.columns) {
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

        definitionErrors.clear(); // Clear state

        Table mainTable = this.accuPath.getInput(); // Loop/scan table

        Range mainRange = mainTable.getIdRange();

        // Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading valuePaths
        List<ColumnPath> paramPaths = this.accuExpr.getParameterPaths();
        Object[] paramValues = new Object[paramPaths.size() + 1]; // Will store valuePaths for all params and current output at the end
        Object result; // Will be written to output for each input

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Find group, that is, projection of the current fact to the group table
            Object g_out = this.accuPath.getValue(i);
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
                this.definitionErrors.add(e);
                return;
            }
            catch(Exception e) {
                this.definitionErrors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
                return;
            }

            // Update output
            this.column.setValue(g, result);
        }
    }

    public ColumnDefinitionAccu(Column column, ColumnPath accuPath, Expression accuExpr) {
        this.column = column;

        this.initDefinition = null;
        this.accuExpr = accuExpr;
        this.finDefinition = null;

        this.accuPath = accuPath;
    }
    public ColumnDefinitionAccu(Column column, ColumnPath accuPath, Expression initExpr, Expression accuExpr, Expression finExpr) {
        this.column = column;

        this.initDefinition = new ColumnDefinitionCalc(column, initExpr);
        this.accuExpr = accuExpr;
        this.finDefinition = new ColumnDefinitionCalc(column, finExpr);

        this.accuPath = accuPath;
    }

}
