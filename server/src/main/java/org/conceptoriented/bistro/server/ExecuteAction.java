package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * This method knows what to do with the data given the input parameters.
 */
@FunctionalInterface
public interface ExecuteAction {
    public void run(Context context) throws BistroError;
}
