package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.server.Action;
import org.conceptoriented.bistro.server.Context;

/**
 * Remove record(s) from a table.
 */
public class ActionRemove implements Action {

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
	public void run() {

		// Read record from the context

		// Add record to the table

	}

	public ActionRemove(Table table) {

	}

}
