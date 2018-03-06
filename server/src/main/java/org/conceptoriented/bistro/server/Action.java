package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * Elementary user-defined action executed against the current state of the data.
 */
public interface Action extends ExecuteAction {

    public Server getServer();
    public void setServer(Server server);

    public Action getNext();
    public void setNext(Action action);

    public void start() throws BistroError;

    public void stop() throws BistroError;

    public void setLambda(ExecuteAction lambda);
}

