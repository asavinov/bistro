package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

import java.util.Map;

/**
 * Add record to a table.
 */
public class ActionAdd implements Action {

    protected Table table;
    protected Map<Column, Object> record;

    @Override
    public void eval(Context context) throws BistroError {
        long id = this.table.add();

        for (Map.Entry<Column, Object> entry : this.record.entrySet()) {
            entry.getKey().setValue(id, entry.getValue());
        }
    }

    public ActionAdd(Table table, Map<Column, Object> record) {
        this.table = table;
        this.record = record;
    }
}
