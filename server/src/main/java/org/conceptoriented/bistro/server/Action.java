package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * It is user-defined function which implements custom data processing logic executed as one step using the parameters in the context.
 */
@FunctionalInterface
public interface Action {
    public void evaluate(Context ctx) throws BistroError;
}
