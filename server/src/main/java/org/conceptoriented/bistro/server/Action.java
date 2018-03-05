package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * Elementary user-defined action executed against the current state of the data.
 */
public interface Action extends Runnable {

    public Context getContext();
    public void setContext(Context context);

    public Action getNext();
    public void setNext(Action action);

    public void start() throws BistroError;

    public void stop() throws BistroError;

    public void setLambda(Runnable lambda);
}
