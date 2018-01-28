package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * Elementary user-defined action executed against the current state of the data.
 */
public interface Action {

	public void start() throws BistroError;

	public void stop() throws BistroError;

	public void setTriggers(Action[] actions) throws BistroError;

    public void exec(Context ctx) throws BistroError;

}
