package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Remove record(s) from a table.
 */
public class TaskRemove extends Task {

    protected void act() {
        long count = (long)this.context.parameters;
        this.context.table.remove(count);
    }

    public TaskRemove(Table table, long count) {
        super(null, null);

        this.context = new Context();
        this.context.table = table;
        this.context.parameters = count;

        this.action = x -> this.act();
    }
}
