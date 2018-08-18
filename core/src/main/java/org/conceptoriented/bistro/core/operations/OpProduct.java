package org.conceptoriented.bistro.core.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.conceptoriented.bistro.core.*;

public class OpProduct implements Operation {

    Table table;

    @Override
    public OperationType getOperationType() {
        return OperationType.PRODUCT;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        // Key-column types have to be populated - we need them to build all their combinations
        List<Column> keyCols = this.table.getKeyColumns();
        keyCols = keyCols.stream().filter(x -> !x.getOutput().isPrimitive()).collect(Collectors.toList()); // Skip all primitive keys
        List<Table> keyTypes = keyCols.stream().map(x -> x.getOutput()).collect(Collectors.toList());
        ret.addAll(keyTypes);

        // All incoming (populating) project-columns (if any)
        List<Column> projCols = this.table.getProjColumns();
        //ret.addAll(projCols);

        // And their input tables which have to be populated before
        List<Table> projTabs = projCols.stream().map(x -> x.getInput()).collect(Collectors.toList());
        //ret.addAll(projTabs);

        return ret;
    }

    @Override
    public void evaluate() {

        // Find all local greater dimensions to be varied (including the super-dim)
        List<Column> keyColumns = this.table.getKeyColumns();
        int colCount = keyColumns.size(); // Dimensionality - how many free dimensions

        // Initialize population
        //for(DcColumn col :cols) {
        //    col.getData().setAutoIndex(false);
        //   col.getData().nullify();
        //}

        //
        // The current state of the search procedure
        //
        long[] offsets = new long[colCount]; // Current id of each dimension (incremented during search)
        for (int i = 0; i < colCount; i++) offsets[i] = -1;

        long[] starts = new long[colCount]; // Start ids for each dimension
        for (int i = 0; i < colCount; i++) starts[i] = keyColumns.get(i).getOutput().getData().getIdRange().start;

        long[] lengths = new long[colCount]; // Length of each dimension (how many ids in each dimension)
        for (int i = 0; i < colCount; i++) lengths[i] = keyColumns.get(i).getOutput().getData().getLength();

        int top = -1; // The current level/top product we change the offset. Depth of recursion.
        do ++top; while (top < colCount && lengths[top] == 0);

        // Alternative recursive iteration: http://stackoverflow.com/questions/13655299/c-sharp-most-efficient-way-to-iterate-through-multiple-arrays-list
        // Alternative: in fact, we can fill each column with integer values alternated periodically depending on its index in the list of columns, e.g., column 0 will always have first half 0s and second half 1s, while next column will alternative two times faster and the last column will always look like 0101010101

        while (top >= 0) {
            if (top == colCount) // New element is ready. Process it.
            {
                List<Object> record = new ArrayList<>();
                for(int i=0; i < offsets.length; i++) {
                    record.add(offsets[i] + starts[i]);
                }

                //
                // Check if this record satisfies the product condition
                //
                boolean whereTrue = true;
                try {
                    whereTrue = this.table.isWhereTrue(record, keyColumns);
                }
                catch(BistroException e) {
                    throw(e);
                }
                catch(Exception e) {
                    throw( new BistroException(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "Error checking where condition.") );
                }

                //
                // Append a new record if necessary
                //
                if(whereTrue == true) {
                    long input = this.table.getData().add();
                    this.table.setValues(input, keyColumns, record);
                }

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

    public OpProduct(Table table) {
        this.table = table;
    }

}
