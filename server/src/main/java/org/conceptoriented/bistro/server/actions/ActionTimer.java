package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Regularly wake up and trigger this action.
 */
public class ActionTimer extends ActionBase {

    Timer timer;
    long period;

    @Override
    public void start() throws BistroError {

        // Configure the timer to regularly wake up and submit this action for execution to the server
        this.timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        // Submit itself
                        ActionTimer.super.server.submit(ActionTimer.this);
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

    public ActionTimer(Server server, long period) {
        super(server);

        this.timer = new Timer();
        this.period = period;
    }
}

class TimerCallback extends TimerTask {
    private ActionTimer actionTimer;
    @Override
    public void run() {
        this.actionTimer.server.submit(this.actionTimer); // Submit the action to the server
    }
    public TimerCallback(ActionTimer actionTimer) {
        this.actionTimer = actionTimer;
    }
}
