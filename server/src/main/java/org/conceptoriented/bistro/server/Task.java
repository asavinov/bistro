package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

import java.util.ArrayList;
import java.util.List;

/**
 * It is a sequence of actions and one context which can be executed by the server within one thread.
 */
public class Task implements Runnable {

    protected List<Action> actions = new ArrayList<>();

    protected Context context;

    @Override
    public void run() { // It will be used by the executor
        for(Action a : this.actions) {
            try {
                a.eval(this.context);
            } catch (BistroError bistroError) {
                bistroError.printStackTrace();
                this.context.server.addError(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Error executing action.", bistroError.message));
                return;
            }
        }
    }

    public Task(List<Action> actions, Context context) {
        this.actions.addAll(actions);
        this.context = context;
    }

    public Task(Action action, Context context) {
        this.actions.add(action);
        this.context = context;
    }
}
