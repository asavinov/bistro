package org.conceptoriented.bistro.server;

import java.util.ArrayList;
import java.util.List;

public class ActionSequence extends ActionBase {

    protected Action start;

    @Override
    public void run() {
        // Sequentially run all actions
        for(Action a = this.start; a != null; a = a.getNext()) {
            a.run();
        }
    }

    public ActionSequence(Server server, Action action) {
        super(server);
        this.start = action;
    }
}
