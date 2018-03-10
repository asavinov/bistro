package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * Connectors are responsible for interaction with the environment by receiving events and sending events.
 * They also are supposed to produce actions and can be invoked from actions executed by the server.
 */
public class Connector {

    public Server server;
    public Server getServer() {
        return this.server;
    }
    public void setServer(Server server) {
        this.server = server;
    }

    public void start() throws BistroError {
    }

    public void stop() throws BistroError {
    }

    public Connector(Server server) {
        this.server = server;
    }
}
