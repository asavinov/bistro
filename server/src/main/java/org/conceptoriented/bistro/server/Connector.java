package org.conceptoriented.bistro.server;

import java.util.List;

import org.conceptoriented.bistro.core.*;

/**
 * Connectors are responsible for interaction with the environment, for example, by receiving events and sending events.
 * They also are supposed to produce actions and can be invoked from actions executed by the server.
 */
public interface Connector {

    public Server getServer();
    public void setServer(Server server);

    // These actions will be executed after the actions of the connector itself in the same task
    public void addAction(Action action);
    public void addActions(List<Action> actions);
    public List<Action> getActions();

    public void start() throws BistroError;
    public void stop() throws BistroError;
}
