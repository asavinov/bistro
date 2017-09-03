package org.conceptoriented.bistro.core;

import java.util.List;

/**
 * It is an implementation of evaluator for calc columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnEvaluatorCalc extends ColumnEvaluatorBase {
	UDE ude;

	@Override
	public void evaluate() {
		// Evaluate calc expression
		if(this.ude == null) { // Default
			super.evaluateExprDefault();
		}
		else {
			super.evaluateExpr(ude, null);
		}
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> deps = super.getExpressionDependencies(this.ude);
		return deps;
	}

	public ColumnEvaluatorCalc(Column column, UDE ude) {
		super(column);
		this.ude = ude;
	}
}
