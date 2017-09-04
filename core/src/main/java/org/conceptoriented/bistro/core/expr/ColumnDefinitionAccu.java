package org.conceptoriented.bistro.core.expr;

import org.conceptoriented.bistro.core.*;

import java.util.List;

/**
 * Representation of a accu column using numeric expression libraries like exp4j (library can be chosen as an option).
 * Link definition is a collection of assignments represented either syntactically or as an (already parsed) array object. 
 */
public class ColumnDefinitionAccu extends ColumnDefinitionBase {

	private String initFormula;
	public String getInitFormula() {
		return this.initFormula;
	}
	private String accuFormula;
	public String getAccuFormula() {
		return this.accuFormula;
	}
	private String finFormula;
	public String getFinFormula() {
		return this.finFormula;
	}
	
	private String accuTable;
	public String getAccuTable() {
		return this.accuTable;
	}
	private String accuPath; // TODO: path should not use any syntactic convention - change this to either one column name or a collection of column names
	public String getAccuPath() {
		return this.accuPath;
	}
	
	@Override
	public ColumnEvaluator translate(Column column) {

		Schema schema = column.getSchema();
		Table inputTable = column.getInput();
		Table outputTable = column.getOutput();


		ColumnPath accuPathColumns = null;

		// Accu table and link (group) path
		Table accuTable = schema.getTable(this.accuTable);
		if(accuTable == null) { // Binding error
			this.errors.add(new BistroError(BistroErrorCode.BIND_ERROR, "Binding error.", "Cannot find table: " + accuTable));
			return null;
		}
		NamePath accuLinkPath = UdeJava.parsePath(this.accuPath); // TODO: path should be given in any syntactic convention and hence no resolution using this convention - change path to a collection/array or one column only
		accuPathColumns = accuLinkPath.resolveColumns(accuTable);
		if(accuPathColumns == null) { // Binding error
			this.errors.add(new BistroError(BistroErrorCode.BIND_ERROR, "Binding error.", "Cannot find columns: " + this.accuPath));
			return null;
		}

		UDE initExpr = null;
		UDE accuExpr = null;
		UDE finExpr = null;

		if(this.formulaKind == ExpressionKind.EXP4J || this.formulaKind == ExpressionKind.EVALEX) {
			// Initialization (always initialize - even for empty formula)
			if(this.initFormula == null || this.initFormula.isEmpty()) { // TODO: We need UDE for constants and for equality (equal to the specified column)
				initExpr = new UdeJava(column.getDefaultValue().toString(), inputTable);
			}
			else {
				initExpr = new UdeJava(this.initFormula, inputTable);
			}
			this.errors.addAll(initExpr.getTranslateErrors());
			if(this.hasErrors()) return null; // Cannot proceed

			// Accumulation
			accuExpr = new UdeJava(this.accuFormula, accuTable);
			this.errors.addAll(accuExpr.getTranslateErrors());
			if(this.hasErrors()) return null; // Cannot proceed

			// Finalization
			if(this.finFormula != null && !this.finFormula.isEmpty()) {
				finExpr = new UdeJava(this.finFormula, inputTable);
				this.errors.addAll(finExpr.getTranslateErrors());
				if(this.hasErrors()) return null; // Cannot proceed
			}
		}
		else if(this.formulaKind == ExpressionKind.UDE) {
			initExpr = super.createInstance(this.initFormula, null);
			accuExpr = super.createInstance(this.accuFormula, null);
			finExpr = super.createInstance(this.finFormula, null);
		}

		if(initExpr == null || accuExpr  == null /* || finExpr == null */) { // TODO: finExpr can be null in the case of no formula. We need to fix this and distinguis between errors and having no formula.
			String frml = "";
			if(initExpr == null) frml = this.initFormula;
			else if(accuExpr == null) frml = this.accuFormula;
			else if(finExpr == null) frml = this.finFormula;
			this.errors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot create expression. " + frml));
			return null;
		}
		
		this.errors.addAll(initExpr.getTranslateErrors());
		this.errors.addAll(accuExpr.getTranslateErrors());
		// this.errors.addAll(finExpr.getTranslateErrors()); // TODO: Fix uncertainty with null expression in the case of no formula and in the case of errors
		if(this.hasErrors()) return null; // Cannot proceed

		// Use these objects to create an evaluator
		ColumnEvaluatorAccu evaluatorAccu = new ColumnEvaluatorAccu(column, initExpr, accuExpr, finExpr, accuPathColumns);

		return evaluatorAccu;
	}

	public ColumnDefinitionAccu(String initFormula, String accuFormula, String finFormula, String accuTable, String accuPath, ExpressionKind formulaKind) {
		this.initFormula = initFormula;
		this.accuFormula = accuFormula;
		this.finFormula = finFormula;
		this.accuTable = accuTable;
		this.accuPath = accuPath;

		super.formulaKind = formulaKind;
	}
}