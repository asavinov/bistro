package org.conceptoriented.bistro.core;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * It is an implementation of evaluator for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnEvaluatorLink extends ColumnEvaluatorBase {
	List<Pair<Column,UDE>> udes = new ArrayList<Pair<Column,UDE>>();

	@Override
	public void evaluate() {
		super.evaluateLink(udes);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<Column>();
		if(udes == null) return ret;

		for(Pair<Column,UDE> pair : udes) {
			Column lhs = pair.getLeft();
			if(!ret.contains(lhs)) ret.add(lhs);

			List<Column> deps = super.getExpressionDependencies(pair.getRight());
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

		List<Pair<Column,UDE>> ude_pairs = new ArrayList<>();
		for(int i=0; i<columns.size(); i++) {
			ude_pairs.add(Pair.of(columns.get(i), udes.get(i)));
		}

		this.udes.addAll(ude_pairs);
	}

	public ColumnEvaluatorLink(Column column, List<Pair<Column,UDE>> udes) {
		super(column);
		this.udes.addAll(udes);
	}
}
