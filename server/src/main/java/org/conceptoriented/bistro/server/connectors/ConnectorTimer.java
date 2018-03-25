package org.conceptoriented.bistro.server.connectors;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Regularly wake up and trigger the specified action.
 * This connector is normally used to execute some regular actions, for example, evaluating the data, deleting unnecessary (old) data or persisting the current state.
 */
public class ConnectorTimer extends ConnectorBase {

    Timer timer;
    long period;

    @Override
    public void start() throws BistroError {

        // This task will be submitted to the server for each timer event
        List<Action> actions = new ArrayList<>();
        actions.addAll(ConnectorTimer.this.getActions());
        Context ctx = new Context();
        ctx.server = super.server;
        ctx.schema = super.server.getSchema();
        Task task = new Task(actions, ctx);

        // This object will be called for each timer event
        TimerTask timerTask =  new TimerTask() {
            @Override
            public void run() {
                // For each timer event, we need to submit a task with all registered timer actions
                ConnectorTimer.super.server.submit(task);
            }
        };

        // Configure the timer to regularly wake up
        this.timer.schedule(
                timerTask,
                this.period,
                this.period
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
