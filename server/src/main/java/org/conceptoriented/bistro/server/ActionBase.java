package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.BistroError;

public class ActionBase implements Action {

    public Server server;

    Context context;
    @Override
    public Context getContext() {
        return this.context;
    }
    @Override
    public void setContext(Context context) {
        this.context = context;
    }

    protected Action next;
    @Override
    public Action getNext(){
        return this.next;
    }
    @Override
    public void setNext(Action action){
        this.next = action;
    }

    @Override
    public void start() throws BistroError {
    }

    @Override
    public void stop() throws BistroError {
    }

    Runnable lambda;
    public void setLambda(Runnable lambda) {
        this.lambda = lambda;
    }

    @Override
    public void run() {
        // The action is really starts executing (in a worker thread). We might want to store real start time here
        if(this.lambda != null) {
            lambda.run();
        }
    }

    public ActionBase(Server server) {
        this.server = server;
    }
}
