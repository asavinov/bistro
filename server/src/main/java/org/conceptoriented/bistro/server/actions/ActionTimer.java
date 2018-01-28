package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.server.Action;
import org.conceptoriented.bistro.server.Context;

/**
 * Regularly wake up and trigger this action.
 */
public class ActionTimer implements Action {

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

	public ActionTimer(long period) {

	}

}
