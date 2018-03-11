package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Regularly wake up and trigger the specified action.
 * This connector is normally used to execute some regular actions, for example, evaluating the data, deleting unnecessary (old) data or persisting the current state.
 */
public class ConnectorTimer extends Connector {

    Timer timer;
    long period;

    protected Action action;
    public void setAction(Action action) {
        this.action = action;
    }
    public Action getAction() {
        return this.action;
    }

    @Override
    public void start() throws BistroError {

        // Configure the timer to regularly wake up and submit this action for execution to the server
        this.timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        // Submit itself
                        ConnectorTimer.super.server.submit(ConnectorTimer.this.action, null);
                    }
                },
                this.period // One time execution in this time
        );

        //this.timer.schedule(new TimerCallback(this), this.period, this.period); // ALternatively, create a dedicated class
    }

    @Override
    public void stop() throws BistroError {
        this.timer.cancel();
    }

    public ConnectorTimer(Server server, long period) {
        super(server);

        this.timer = new Timer("Bistro Server: ConnectorTimer Timer");
        this.period = period;
    }
}

class TimerCallback extends TimerTask {
    private ConnectorTimer actionTimer;
    @Override
    public void run() {
        this.actionTimer.server.submit(this.actionTimer.action, null); // Submit the action to the server
    }
    public TimerCallback(ConnectorTimer actionTimer) {
        this.actionTimer = actionTimer;
    }
}
