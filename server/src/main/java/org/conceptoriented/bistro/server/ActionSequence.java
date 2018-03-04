package org.conceptoriented.bistro.server;

import java.util.ArrayList;
import java.util.List;

public class ActionSequence extends ActionBase {
    List<Action> actions = new ArrayList<>();

    @Override
    public void run() {
        // Sequentially run all actions
        for (Action a : this.actions) {
            a.run();
        }
    }

    public ActionSequence(Action action) {
        this.actions.add(action);
    }
    public ActionSequence(Action[] actions) {
        for (Action a : actions) {
            this.actions.add(a);
        }
    }
}
