package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The logic of evaluation of rolling columns.
 */
public class ColumnDefinitionRoll implements ColumnDefinition {

	Column column;

    EvaluatorAccu lambda;
	ColumnPath[] paths;

    List<BistroError> errors = new ArrayList<>();

	@Override
	public List<BistroError> getErrors() {
		return this.errors;
	}

    @Override
    public List<Element> getDependencies() {
        return null;
    }

    @Override
    public void eval() {

    }

    public ColumnDefinitionRoll(Column column, ColumnPath rollPath, double size, EvaluatorAccu lambda, ColumnPath[] paths) {
        this.column = column;

        this.lambda = lambda;
        this.paths = paths;
    }

    public ColumnDefinitionRoll(Column column, Column rollColumn, double size, EvaluatorAccu lambda, Column[] columns) {
        this.column = column;

        this.paths = new ColumnPath[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.paths[i] = new ColumnPath(columns[i]);
        }
    }

}
