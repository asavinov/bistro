package org.conceptoriented.bistro.core.deprecated;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.core.expr.ExpressionKind;
import org.conceptoriented.bistro.core.expr.UdeJava;

/**
 * Representation of a calc column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Calc definition is an expression which uses other column paths as parameters. Or it can be a single assignment where lhs is this column. 
 */
public class ColumnDefinitionCalc extends ColumnDefinitionBase {

	String formula;
	public String getFormula() {
		return this.formula;
	}
	
	@Override
	public ColumnEvaluator translate(Column column) {
		
		Schema schema = column.getSchema();
		Table inputTable = column.getInput();

		UDE expr = null; // We need only one expression

		if(this.formulaKind == ExpressionKind.EXP4J || this.formulaKind == ExpressionKind.EVALEX) {
			expr = new UdeJava(this.formula, inputTable);
		}
		else if(this.formulaKind == ExpressionKind.UDE) {
			expr = super.createInstance(this.formula,null);
		}
		else {
			; // TODO: Error not implemented
		}
		
		if(expr == null) {
			this.errors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot create expression. " + this.formula));
			return null;
		}
		
		this.errors.addAll(expr.getTranslateErrors());
		if(this.hasErrors()) return null; // Cannot proceed

		ColumnEvaluatorCalc evaluatorCalc = new ColumnEvaluatorCalc(column, expr);

		return evaluatorCalc;
	}

	public ColumnDefinitionCalc(String formula, ExpressionKind formulaKind) {
		this.formula = formula;
		super.formulaKind = formulaKind;
	}
}