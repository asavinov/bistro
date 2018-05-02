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
    EvaluatorAccu adder;
    EvaluatorAccu remover;
    ColumnDefinitionCalc finDefinition;

    ColumnPath[] paths;

    List<BistroError> errors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.errors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> deps = new ArrayList<>();

        deps.add(this.column.getInput()); // Columns depend on their input table

        deps.add(this.groupPath.getInput()); // Accumulate column depends on the fact table

        for(Column col : this.groupPath.columns) {
            if(!deps.contains(col)) deps.add(col);
        }

        // Initializer dependencies
        if(this.initDefinition != null) {
            for(Element dep : this.initDefinition.getDependencies()) {
                if(!deps.contains(dep)) deps.add(dep);
            }
        }

        // Updater dependencies
        if(this.paths != null && (this.adder != null || this.remover != null)) {
            for(ColumnPath path : this.paths) {
                for(Column col : path.columns) {
                    if(!deps.contains(col)) deps.add(col);
                }
            }
        }

        // Finalizer dependencies
        if(this.finDefinition != null) {
            for(Element d : this.finDefinition.getDependencies()) {
                if(!deps.contains(d)) deps.add(d);
            }
        }

        return deps;
    }

    @Override
    public void evaluate() {

        errors.clear(); // Clear state

        //
        // Initialize new elements
        //
        this.evalInitialier();

        //
        // Update group elements by data from added/removed facts
        //
        Table mainTable = this.groupPath.getInput(); // Loop/scan table - fact table (not the table where this column is defined)

        if(this.remover != null) {
            Range removedRange = mainTable.getRemovedRange();
            this.evalUpdater(removedRange, this.remover);
        }
        if(this.adder != null) {
            Range addedRange = mainTable.getAddedRange();
            this.evalUpdater(addedRange, this.adder);
        }

        //Range mainRange = mainTable.getIdRange();
        //this.evalUpdater(mainRange, this.adder);

        //
        // Finalization
        //
        // TODO: Remove finalizer mechanism for simplicity
        if(this.finDefinition != null) { // Default
            this.finDefinition.evaluate();
        }
        else {
            ; // Skip finalization if not specified
        }
    }

    protected void evalInitialier() {

        // TODO: We need to initialize ONLY if the initializer expression is dirty (e.g., group table changed)
        //   -> and then initialized records (possibly all) have to be FULLY (not incrementally) re-evaluated

        // Initializer dependencies
        List<Element> deps = new ArrayList<>();
        boolean isColumnDepChanged = false;
        if(this.initDefinition != null) {
            for(Element dep : this.initDefinition.getDependencies()) {
                if(deps.contains(dep)) continue;
                deps.add(dep);
                if(dep instanceof Column && ((Column)dep).isChanged()) isColumnDepChanged = true;
            }
        }

        Table mainTable = this.column.getInput();
        if(isColumnDepChanged) { // Initialize all
            this.initDefinition.evaluate();
        }
        else if(mainTable.isChanged()) {
            Range addedRange = mainTable.getAddedRange();
            if(this.initDefinition != null) {
                // TODO: set or initialize values for only added range. In evaluate() it has to be done automatically
                this.initDefinition.evaluate();
            }
            else {
                this.column.setValue(); // TODO: we need a method setValues for a range of ids
            }
        }
    }

    protected void evalUpdater(Range mainRange, EvaluatorAccu lambda) {

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
                result = lambda.evaluate(aggregate, paramValues);
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

    public ColumnDefinitionAccu(Column column, ColumnPath groupPath, EvaluatorAccu adder, ColumnPath[] paths) {
        this.column = column;

        this.groupPath = groupPath;

        this.initDefinition = null;
        this.adder = adder;
        this.finDefinition = null;

        this.paths = paths;
    }

    public ColumnDefinitionAccu(Column column, Column groupColumn, EvaluatorAccu adder, Column[] columns) {
        this.column = column;

        this.groupPath = new ColumnPath(groupColumn);

        this.initDefinition = null;
        this.adder = adder;
        this.finDefinition = null;

        this.paths = new ColumnPath[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.paths[i] = new ColumnPath(columns[i]);
        }
    }

    public ColumnDefinitionAccu(Column column, ColumnPath groupPath, EvaluatorCalc initializer, EvaluatorAccu adder, EvaluatorCalc finalizer, ColumnPath[] paths) {
        this.column = column;

        this.groupPath = groupPath;

        this.initDefinition = new ColumnDefinitionCalc(column, initializer, paths);
        this.adder = adder;
        this.finDefinition = new ColumnDefinitionCalc(column, finalizer, paths);

        this.paths = paths;
    }

}
