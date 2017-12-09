package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TableDefinitionProd implements TableDefinition {

    Table table;

    List<BistroError> definitionErrors = new ArrayList<>();

    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        // Key-column types have to be populated - we need them to build all their combinations
        List<Column> keyCols = this.table.getKeyColumns();
        List<Table> keyTypes = keyCols.stream().map(x -> x.getOutput()).collect(Collectors.toList());
        ret.addAll(keyTypes);

        // All incoming (populating) proj-columns
        List<Column> projCols = this.table.getProjColumns();
        // And their input tables which have to be populated before
        List<Table> projTabs = projCols.stream().map(x -> x.getInput()).collect(Collectors.toList());

        ret.addAll(projCols);
        ret.addAll(projTabs);

        return ret;
    }

    @Override
    public void populate() {

        // Find all local greater dimensions to be varied (including the super-dim)
        List<Column> cols = this.table.getKeyColumns();
        int colCount = cols.size(); // Dimensionality - how many free dimensions

        // Initialize population
        //for(DcColumn col :cols) {
        //    col.getData().setAutoIndex(false);
        //   col.getData().nullify();
        //}

        //
        // Prepare value paths for where evaluation (see also ColumnDefinitionLink)
        //
        Expression whereExpr = this.table.expressionWhere;
        List<ColumnPath> wherePaths =  null;
        Object[] whereValues = null;
        if(whereExpr != null) {
            wherePaths =  whereExpr.getParameterPaths();
            whereValues = new Object[wherePaths.size() + 1];
        }

        boolean whereTrue = true; // Where result for the last appended record

        //
        // The current state of the search procedure
        //
        long[] offsets = new long[colCount]; // Current id of each dimension (incremented during search)
        for (int i = 0; i < colCount; i++) offsets[i] = -1;

        long[] starts = new long[colCount]; // Start ids for each dimension
        for (int i = 0; i < colCount; i++) starts[i] = cols.get(i).getOutput().getIdRange().start;

        long[] lengths = new long[colCount]; // Length of each dimension (how many ids in each dimension)
        for (int i = 0; i < colCount; i++) lengths[i] = cols.get(i).getOutput().getLength();

        int top = -1; // The current level/top where we change the offset. Depth of recursion.
        do ++top; while (top < colCount && lengths[top] == 0);

        // Alternative recursive iteration: http://stackoverflow.com/questions/13655299/c-sharp-most-efficient-way-to-iterate-through-multiple-arrays-list
        // Alternative: in fact, we can fill each column with integer values alternated periodically depending on its index in the list of columns, e.g., column 0 will always have first half 0s and second half 1s, while next column will alternative two times faster and the last column will always look like 0101010101

        long input = -1; // If -1, then we need to append a record. If >=0, then this id has to be used as a new record (previous append does not satisfy where expression but was not deleted).
        while (top >= 0) {
            if (top == colCount) // New element is ready. Process it.
            {
                // Append a new record if necessary
                if(whereTrue == true) {
                    input = this.table.add();
                }
                // Initialize the new record
                for (int i = 0; i < colCount; i++) {
                    cols.get(i).setValue(input, offsets[i] + starts[i]);
                }

                // TODO: Switch to testing a record *before* adding using ColumnPath::FirstSegment method
                // TODO: Unify checking whereExpr with proj-column append
                if(whereExpr != null) {

                    // Read all parameters
                    for(int p=0; p < wherePaths.size(); p++) {
                        whereValues[p] = wherePaths.get(p).getValue(input);
                    }

                    // Evaluate
                    try {
                        whereTrue = (boolean) whereExpr.eval(whereValues);
                    }
                    catch(BistroError e) {
                        this.definitionErrors.add(e);
                        return;
                    }
                    catch(Exception e) {
                        this.definitionErrors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
                        return;
                    }
                }
                else {
                    whereTrue = true;
                    input = -1;
                }
                // We do not delete the record if it does not satisfy where condition - it will be reused on the next step.

                top--;
                while (top >= 0 && lengths[top] == 0) // Go up by skipping empty dimensions and reseting
                { offsets[top--] = -1; }
            }
            else
            {
                // Find the next valid offset
                offsets[top]++;

                if (offsets[top] < lengths[top]) // Offset chosen
                {
                    do ++top;
                    while (top < colCount && lengths[top] == 0); // Go up (forward) by skipping empty dimensions
                }
                else // Level is finished. Go back.
                {
                    do { offsets[top--] = -1; }
                    while (top >= 0 && lengths[top] == 0); // Go down (backward) by skipping empty dimensions and reseting
                }
            }
        }

        if(whereTrue == false) {
            this.table.remove(-1); // Delete last record which does not satisfies where condition
        }

        // Finalize population
        //for(DcColumn col :cols) {
        //    col.getData().reindex();
        //    col.getData().setAutoIndex(true);
        //}

    }

    public TableDefinitionProd(Table table) {
        this.table = table;
    }

}
