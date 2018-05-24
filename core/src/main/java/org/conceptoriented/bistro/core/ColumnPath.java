package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A sequence of keyColumns where each next column starts from the data type (table) of the previous column.
 */
public class ColumnPath {

    public List<Column> columns = new ArrayList<>();

    public Object getValue(long id) {
        int len = this.columns.size();
        Object out = id;
        for(int i = 0; i < len; i++) {
            Column col = this.columns.get(i);
            out = col.getValue((long)out); // Previously obtained id is supposed to be valid

            if(i == len - 1) { // Do not check the output for the very last segment (return as is)
                break;
            }

            // New retrieved output might not be valid id (null or -1) and then it cannot be used in next segment
            if (out == null) break;
            if ((long)out < 0) break;
        }
        return out;
    }

    // Skip first segment and use the argument is its output
    public Object getValueSkipFirst(Object firstOutput) {
        int len = this.columns.size();
        Object out = firstOutput;
        if(len > 1) {
            for(int i = 1; i < len; i++) {
                Column col = this.columns.get(i);
                out = col.getValue((long)out);

                if(i == len - 1) { // Do not check the output for the very last segment (return as is)
                    break;
                }

                // New retrieved output might not be valid id (null or -1) and then it cannot be used in next segment
                if (out == null) break;
                if ((long)out < 0) break;
            }
        }
        return out;
    }

    public int size() {
        return this.columns.size();
    }

    public Table getInput() {
        if(this.columns.size() == 0) return null;
        return this.columns.get(0).getInput();
    }

    public Table getOutput() {
        if(this.columns.size() == 0) return null;
        return this.columns.get(this.columns.size()-1).getOutput();
    }

    public static List<Column> getColumns(List<ColumnPath> paths) { // Extract (unique) columns from a list of valuePaths
        List<Column> columns = new ArrayList<>();

        for(ColumnPath path : paths) {
            for(Column col : path.columns) {
                if(!columns.contains(col)) {
                    columns.add(col);
                }
            }
        }
        return columns;
    }

    public static ColumnPath create(NamePath namePath, Table table) {
        ColumnPath path = new ColumnPath();
        for(String name : namePath.names) {
            Column col = table.getColumn(name);
            if(col == null) {
                return path; // This path has shorter length
            }
            path.columns.add(col);
        }
        return path;
    }

    public ColumnPath(Column... columns) {
        this.columns.addAll(Arrays.asList(columns));
    }

    public ColumnPath(List<Column> columns) {
        this.columns.addAll(columns);
    }

    public ColumnPath(Column column) {
        this.columns.add(column);
    }

    public ColumnPath() {
    }

}
