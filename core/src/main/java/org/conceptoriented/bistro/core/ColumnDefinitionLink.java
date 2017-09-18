package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * It is an implementation of definition for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnDefinitionLink extends ColumnDefinitionBase {
	List<Column> columns = new ArrayList<>();
	List<Expression> exprs = new ArrayList<>();

	@Override
	public void eval() {
		super.evaluateLink(this.columns, this.exprs);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<>();
		if(exprs == null) return ret;

		for(Column col : this.columns) {
			if (!ret.contains(col)) ret.add(col);
		}

		for(Expression expr : this.exprs) {
			List<Column> deps = ColumnPath.getColumns(expr.getParameterPaths());
			for(Column col : deps) {
				if(!ret.contains(col)) {
					ret.add(col);
				}
			}
		}
		return ret;
	}

	public ColumnDefinitionLink(Column column, List<Column> columns, List<Expression> exprs) {
		super(column);

        this.columns.addAll(columns);
		this.exprs.addAll(exprs);
	}

}
