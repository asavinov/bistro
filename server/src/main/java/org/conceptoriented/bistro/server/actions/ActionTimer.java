package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Regularly wake up and trigger this action.
 */
public class ActionTimer extends ActionBase {

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

	public ActionTimer(long period) {

	}

}
