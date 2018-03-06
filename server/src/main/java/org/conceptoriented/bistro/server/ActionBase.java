package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.*;

public class ActionBase implements Action {

    public Server server;
    @Override
    public Server getServer() {
        return this.server;
    }
    @Override
    public void setServer(Server server) {
        this.server = server;
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

    ExecuteAction lambda;
    @Override
    public void setLambda(ExecuteAction lambda) {
        this.lambda = lambda;
    }

    @Override
    public void run(Context context) throws BistroError {
        // The action is really starts executing (in a worker thread). We might want to store real start time here
        if(this.lambda != null) {
            lambda.run(context);
        }
    }

    public ActionBase(Server server) {
        this.server = server;
    }
}
