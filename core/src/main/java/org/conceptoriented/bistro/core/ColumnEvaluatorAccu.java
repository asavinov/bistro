package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnEvaluatorAccu extends ColumnEvaluatorBase {

	UDE initExpr;
	UDE accuExpr;
	UDE finExpr;

	ColumnPath accuPathColumns;

	@Override
	public void evaluate() {
		// Initialization
		if(this.initExpr == null) { // Default
			super.evaluateExprDefault();
		}
		else {
			super.evaluateExpr(this.initExpr, null);
		}

		// Accumulation
		super.evaluateExpr(this.accuExpr, this.accuPathColumns);

		// Finalization
		if(this.finExpr == null) { // Default
			; // No finalization if not specified
		}
		else {
			super.evaluateExpr(this.finExpr, null);
		}
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<Column>();

		if(this.initExpr != null) {
			for(Column col : super.getExpressionDependencies(this.initExpr)) {
				if(!ret.contains(col)) ret.add(col);
			}
		}
		if(this.accuExpr != null) {
			for(Column col : super.getExpressionDependencies(this.accuExpr)) {
				if(!ret.contains(col)) ret.add(col);
			}
		}
		if(this.finExpr != null) {
			for(Column col : super.getExpressionDependencies(this.finExpr)) {
				if(!ret.contains(col)) ret.add(col);
			}
		}

		for(Column col : this.accuPathColumns.columns) {
			if(!ret.contains(col)) ret.add(col);
		}

		return ret;
	}

	public ColumnEvaluatorAccu(Column column, UDE initExpr, UDE accuExpr, UDE finExpr, ColumnPath accuPathColumns) {
		super(column);

		this.initExpr = initExpr;
		this.accuExpr = accuExpr;
		this.finExpr = finExpr;

		this.accuPathColumns = accuPathColumns;
	}
}
