package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

public class ColumnPath {

    public List<Column> columns = new ArrayList<>();

    public Object getValue(int it) {
        return null;
    }

    public ColumnPath resolve(NamePath namePath, Table table) {
        return this;
    }

    public static ColumnPath create(NamePath namePath, Table table) {
        return new ColumnPath();
    }

}
