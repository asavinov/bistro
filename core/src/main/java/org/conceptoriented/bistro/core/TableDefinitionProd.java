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
        ret.addAll(this.getKeyColumns());

        // All incoming (populating) proj-columns
        List<Column> projCols = this.getProjColumns();
        // And their input tables which have to be populated before
        List<Table> projTabs = projCols.stream().map(x -> x.getInput()).collect(Collectors.toList());

        ret.addAll(projCols);
        ret.addAll(projTabs);

        return null;
    }

    @Override
    public void populate() {

        // Find all local greater dimensions to be varied (including the super-dim)
        List<Column> cols = this.getKeyColumns();
        int colCount = cols.size(); // Dimensionality - how many free dimensions

        // Initialize population
        //for(DcColumn col :cols) {
        //    col.getData().setAutoIndex(false);
        //   col.getData().nullify();
        //}

        //
        // The current state of the search procedure
        //
        long[] lengths = new long[colCount]; // Size of each dimension being varied (how many offsets in each dimension)
        for (int i = 0; i < colCount; i++) lengths[i] = cols.get(i).getOutput().getLength();

        long[] offsets = new long[colCount]; // The current point/offset for each dimension during search
        for (int i = 0; i < colCount; i++) offsets[i] = -1; // TODO: We need to set to the start of the range

        int top = -1; // The current level/top where we change the offset. Depth of recursion.
        do ++top; while (top < colCount && lengths[top] == 0);

        // Alternative recursive iteration: http://stackoverflow.com/questions/13655299/c-sharp-most-efficient-way-to-iterate-through-multiple-arrays-list
        while (top >= 0)
        {
            if (top == colCount) // New element is ready. Process it.
            {
                // Initialize a record and append it
                long input = this.table.add();
                for (int i = 0; i < colCount; i++) {
                    cols.get(i).setValue(input, offsets[i]);
                    // OLD: vals[i] = offsets[i];
                }

                // TODO: Check if the new appended instance satisfies the where condition and if not then remove it

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

        // Finalize population
        //for(DcColumn col :cols) {
        //    col.getData().reindex();
        //    col.getData().setAutoIndex(true);
        //}

    }

    protected List<Column> getKeyColumns() { // Get all columns the domains of which have to be combined (non-primitive key-columns)
        List<Column> ret = new ArrayList<>();
        for(Column col : this.table.getColumns()) {
            if(col.getDefinitionType() != ColumnDefinitionType.KEY) continue; // Skip non-key columns
            if(col.getOutput().isPrimitive()) continue; // Skip primitive columns
        }
        return ret;
    }

    protected List<Column> getProjColumns() { // Get all incoming proj-columns
        List<Column> ret = new ArrayList<>();
        for(Column col : this.table.getSchema().getColumns()) {
            if(col.getOutput() != this.table) continue;
            if(col.getDefinitionType() != ColumnDefinitionType.PROJ) continue; // Skip non-key columns
        }
        return ret;
    }

    public TableDefinitionProd(Table table) {
        this.table = table;
    }

}
