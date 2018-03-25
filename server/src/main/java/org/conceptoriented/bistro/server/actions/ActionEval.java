package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Evaluate schema.
 */
public class ActionEval implements Action {

    protected Schema schema;

    @Override
    public void eval(Context context) throws BistroError {
        this.schema.eval();
    }

    public ActionEval(Schema schema) {
        this.schema = schema;
    }
}
