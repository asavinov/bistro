package org.conceptoriented.bistro.server.actions;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Evalute schema.
 */
public class TaskEval extends Task {

	protected void act() {
		this.context.schema.eval();
	}

	public TaskEval(Schema schema) {
		super(null, null);

		this.context = new Context();
		this.context.schema = schema;

		this.action = x -> this.act();
	}
}
