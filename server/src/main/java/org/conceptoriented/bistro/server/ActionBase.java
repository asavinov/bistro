package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.BistroError;

public class ActionBase implements Action {

    Context context;
    @Override
    public Context getContext() {
        return this.context;
    }
    @Override
    public void setContext(Context context) {
        this.context = context;
    }

    @Override
    public void start() throws BistroError {
    }

    @Override
    public void stop() throws BistroError {
    }

    @Override
    public void setTriggers(Action[] actions) throws BistroError {
    }

    @Override
    public void run() {
    }

}
