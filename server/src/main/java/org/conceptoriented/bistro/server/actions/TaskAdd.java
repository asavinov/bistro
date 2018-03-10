package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

import java.util.Map;

/**
 * Add record to a table.
 */
public class TaskAdd extends Task {

    protected void act() {

        long id = this.context.table.add();

        Map<Column, Object> record = (Map<Column, Object>)this.context.parameters;
        for (Map.Entry<Column, Object> entry : record.entrySet()) {
            entry.getKey().setValue(id, entry.getValue());
        }
    }

    public TaskAdd(Table table, Map<Column, Object> record) {
        super(null, null);

        this.context = new Context();
        this.context.table = table;
        this.context.parameters = record;

        this.action = x -> this.act();
    }
}
