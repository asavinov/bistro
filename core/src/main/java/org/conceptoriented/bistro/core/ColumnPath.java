package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * A sequence of keyColumns where each next column starts from the data type (table) of the previous column.
 */
public class ColumnPath {

    public List<Column> columns = new ArrayList<>();

    public Object getValue(long id) {
        Object out = id;
        for(Column col : columns) {
            out = col.getValue((long)out);
            if(out == null) break;
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

    public static List<Column> getColumns(List<ColumnPath> paths) { // Extract (unique) keyColumns from a list of valuePaths
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

    public ColumnPath(List<Column> columns) {
        this.columns.addAll(columns);
    }

    public ColumnPath(Column column) {
        this.columns.add(column);
    }

    public ColumnPath() {
    }

}
