package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * It is an implementation of definition for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnDefinitionAccu extends ColumnDefinitionBase {

	Expression initExpr;
	Expression accuExpr;
	Expression finExpr;

	ColumnPath accuPathColumns;

	@Override
	public void eval() {
		// Initialization
		if(this.initExpr == null) { // Default
			super.column.setValue(); // Reset
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
		List<Column> ret = new ArrayList<>();

		if(this.initExpr != null) {
			for(Column col : ColumnPath.getColumns(this.initExpr.getParameterPaths())) {
				if(!ret.contains(col)) ret.add(col);
			}
		}
		if(this.accuExpr != null) {
			for(Column col : ColumnPath.getColumns(this.accuExpr.getParameterPaths())) {
				if(!ret.contains(col)) ret.add(col);
			}
		}
		if(this.finExpr != null) {
			for(Column col : ColumnPath.getColumns(this.finExpr.getParameterPaths())) {
				if(!ret.contains(col)) ret.add(col);
			}
		}

		for(Column col : this.accuPathColumns.columns) {
			if(!ret.contains(col)) ret.add(col);
		}

		return ret;
	}

	public ColumnDefinitionAccu(Column column, Expression initExpr, Expression accuExpr, Expression finExpr, ColumnPath accuPathColumns) {
		super(column);

		this.initExpr = initExpr;
		this.accuExpr = accuExpr;
		this.finExpr = finExpr;

		this.accuPathColumns = accuPathColumns;
	}
}
