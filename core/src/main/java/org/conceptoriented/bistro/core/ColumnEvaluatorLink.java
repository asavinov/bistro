package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.expr.UDE;

import java.util.ArrayList;
import java.util.List;

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnEvaluatorLink extends ColumnEvaluatorBase {
	List<Column> columns = new ArrayList<>();
	List<UDE> udes = new ArrayList<>();

	@Override
	public void evaluate() {
		super.evaluateLink(this.columns, this.udes);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<>();
		if(udes == null) return ret;

		for(Column col : this.columns) {
			if (!ret.contains(col)) ret.add(col);
		}

		for(UDE ude : this.udes) {
			List<Column> deps = super.getExpressionDependencies(ude);
			for(Column col : deps) {
				if(!ret.contains(col)) {
					ret.add(col);
				}
			}
		}
		return ret;
	}

	public ColumnEvaluatorLink(Column column, List<Column> columns, List<UDE> udes) {
		super(column);

        this.columns.addAll(columns);
		this.udes.addAll(udes);
	}

}
