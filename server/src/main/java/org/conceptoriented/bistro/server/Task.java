package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

/**
 * It is a combination of an action and context which can be submitted to a server for execution.
 */
public class Task implements Action, Runnable {

    protected Action action;

    protected Context context;

    @Override
    public void eval(Context context) throws BistroError {
        if(this.action != null) {
            this.action.eval(this.context);
        }
    }

    @Override
    public void run() { // It will be used by the executor
        try {
            this.action.eval(this.context);
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            this.context.server.addError(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Error executing action.", bistroError.message));
            return;
        }
    }

    public Task(Action action, Context context) {
        this.action = action;
        this.context = context;
    }
}
