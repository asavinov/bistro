package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

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

    public int getLength() {
        return this.columns.size();
    }

    public Table getInput() {
        // TODO: Check size
        return this.columns.get(0).getInput();
    }

    public Table getOutput() {
        // TODO: Check size
        return this.columns.get(this.columns.size()-1).getOutput();
    }

    public ColumnPath resolve(NamePath namePath, Table table) {
        return this;
    }

    public static ColumnPath create(NamePath namePath, Table table) {
        return new ColumnPath();
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
