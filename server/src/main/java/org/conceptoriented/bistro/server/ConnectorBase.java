package org.conceptoriented.bistro.server;

import java.util.ArrayList;
import java.util.List;

public class ConnectorBase implements Connector {

    public Server server;
    @Override
    public Server getServer() {
        return this.server;
    }
    @Override
    public void setServer(Server server) {
        this.server = server;
    }

    List<Action> actions = new ArrayList<>();
    @Override
    public void addAction(Action action) {
        this.actions.add(action);
    }
    @Override
    public void addActions(List<Action> actions) {
        this.actions.addAll(actions);
    }
    @Override
    public List<Action> getActions() {
        return this.actions;
    }

    @Override
    public void start() {
    }
    @Override
    public void stop() {
    }

    public ConnectorBase(Server server) {
        this.server = server;
    }
}
