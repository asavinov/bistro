package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Evaluate schema.
 */
public class ActionEval implements Action {

    protected Schema schema;

    @Override
    public void evaluate(Context ctx) throws BistroError {
        this.schema.evaluate();
    }

    public ActionEval(Schema schema) {
        this.schema = schema;
    }
}
