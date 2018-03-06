package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

public class Task implements Runnable {

    protected Context context;
    public Context getContext() {
        return context;
    }
    public void setContext(Context context) {
        this.context = context;
    }

    protected Action start;

    @Override
    public void run() {
        // Sequentially run all actions
        for(Action a = this.start; a != null; a = a.getNext()) {
            try {
                a.run(this.context);
            } catch (BistroError bistroError) {
                bistroError.printStackTrace();
                a.getServer().addError(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Error executing action.", bistroError.message));
                return;
            }
        }
    }

    public Task(Server server, Action action) {
        this.start = action;
    }
}
