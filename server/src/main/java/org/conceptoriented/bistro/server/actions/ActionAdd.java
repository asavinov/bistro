package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Add record to a table.
 */
public class ActionAdd implements Action {

	@Override
	public void start() throws BistroError {
	}

	@Override
	public void stop() throws BistroError {
	}

	@Override
    public void setTriggers(Action[] actions) throws BistroError {
	}

	@Override
    public void exec(Context ctx) throws BistroError {

	    // Read record from the context

        // Add record to the table

	}

	public ActionAdd(Table table) {

	}

}
