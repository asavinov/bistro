package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.expr.ColumnDefinitionAccu;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionCalc;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionLink;
import org.conceptoriented.bistro.core.expr.ExpressionKind;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class Column {
	private Schema schema;
	public Schema getSchema() {
		return this.schema;
	}
	
	private final UUID id;
	public UUID getId() {
		return this.id;
	}

	private String name;
	public String getName() {
		return this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	private Table input;
	public Table getInput() {
		return this.input;
	}
	public void setInput(Table table) {
		this.input = table;
	}

	private Table output;
	public Table getOutput() {
		return this.output;
	}
	public void setOutput(Table table) {
		this.output = table;
	}

	private boolean key = false;
	public boolean isKey() {  return this.key; }

    //
    // Data dirty state: added/removed inputs (set population), and change outputs (function updated)
    //

    protected boolean isChanged = false;

    //
	// Data (public)
	//

    private ColumnData data;

    public Object getValue(long id) { return this.data.getValue(id); }

	public void setValue(long id, Object value) { this.data.setValue(id, value); this.isChanged = true; }

    //
    // Data (protected). These are used from Table only (all columns change their ranges simultaniously) and from users - users add/remove elements via tables.
    //

    protected void add() { this.data.add(); this.isChanged = true; }
	protected void add(long count) { this.data.add(count); this.isChanged = true; }

    protected void remove() { this.data.remove(1); this.isChanged = true; }
    protected void remove(long count) { this.data.remove(count); this.isChanged = true; }



    @Deprecated // This probably should also moved to UtilsData which knows about all data types, conversions, default values etc.
	public Object getDefaultValue() { // TODO: Depends on the column type. Maybe move to Data class or to Evaluator/Definition
		Object defaultValue;
		if(this.getOutput().isPrimitive()) {
			defaultValue = 0.0;
		}
		else {
			defaultValue = null;
		}
		return defaultValue;
	}

    @Deprecated // Convenience method. Should be part of UtilsData with casts/conversions. Replace it everywhere by add() and setValue()
	public long appendValue(Object value) {
        // Cast the value to type of this column
        if(this.getOutput().getName().equalsIgnoreCase("String")) {
            try { value = value.toString(); }
            catch (Exception e) { value = ""; }
        }
        else if(this.getOutput().getName().equalsIgnoreCase("Double") || this.getOutput().getName().equalsIgnoreCase("Integer")) {
            if(value instanceof String) {
                try { value = NumberFormat.getInstance(Locale.US).parse(((String)value).trim()); }
                catch (ParseException e) { value = Double.NaN; }
            }
        }

        this.data.add(); // TODO: it does not work. we need to get largest (new) id but we can do it only from the table, so remove this method
        this.data.setValue(0, value);
        return 0;
    }

    //
	// Formula kind
	//
	
	protected ColumnKind kind;
	public ColumnKind getKind() {
		return this.kind;
	}
	public void setKind(ColumnKind kind) {
		this.kind = kind;
	}
	public boolean isDerived() {
		if(this.kind == ColumnKind.CALC || this.kind == ColumnKind.ACCU || this.kind == ColumnKind.LINK || this.kind == ColumnKind.CLASS) {
			return true;
		}
		return false;
	}

	//
	// Three formula types
	//
	
	// It is used for all definition types (by default) but every definition has its own expression kind
	public ExpressionKind expressionKind = ExpressionKind.EXP4J;

	// Calc formula
	protected ColumnDefinitionCalc definitionCalc;
	public ColumnDefinitionCalc getDefinitionCalc() {
		return this.definitionCalc;
	}
	public void setDefinitionCalc(ColumnDefinitionCalc definition) {
		this.definitionCalc = definition;
		this.setFormulaChange(true);
	}

	// Link formula
	protected ColumnDefinitionLink definitionLink;
	public ColumnDefinitionLink getDefinitionLink() {
		return this.definitionLink;
	}
	public void setDefinitionLink(ColumnDefinitionLink definition) {
		this.definitionLink = definition;
		this.setFormulaChange(true);
	}

	//
	// Accumulation formula
	//
	protected ColumnDefinitionAccu definitionAccu;
	public ColumnDefinitionAccu getDefinitionAccu() {
		return this.definitionAccu;
	}
	public void setDefinitionAccu(ColumnDefinitionAccu definition) {
		this.definitionAccu = definition;
		this.setFormulaChange(true);
	}
	
	//
	// Formula dirty status (own or inherited)
	//

	/**
	 * Status of the data defined by this (and only this) column formula: clean (up-to-date) or dirty.
	 * This status is cleaned by evaluating this column and it made dirty by setting (new), resetting (delete) or changing (updating) the formula.
	 * It is an aggregated status for new, deleted or changed formulas.
	 * It is own status of this columns only (not inherited/propagated).
	 */
	public boolean isFormulaDirty() {
		return this.formulaChange || this.formulaNew || this.formulaDelete;
	}
	public void setFormulaClean() {
		this.formulaChange = false;
		this.formulaNew = false;
		this.formulaDelete = false;
	}

	private boolean formulaChange = false; // Formula has been changed
	public void setFormulaChange(boolean dirty) {
		this.formulaChange = dirty;
	}
	private boolean formulaNew = false; // Formula has been added
	public void setFormulaNew(boolean dirty) {
		this.formulaNew = dirty;
	}
	private boolean formulaDelete = false; // Formula has been deleted
	public void setFormulaDelete(boolean dirty) {
		this.formulaDelete = dirty;
	}

	//
	// Formula (translate) dependencies
	//
	
	/**
	 * All other columns it directly depends on. These columns are directly used in its formula to compute output.
	 */
	protected List<Column> dependencies = new ArrayList<Column>();
	public List<Column> getDependencies() {
		return this.dependencies;
	}
	public void setDependencies(List<Column> deps) {
		resetDependencies();
		this.dependencies.addAll(deps);
	}
	public void resetDependencies() {
		this.dependencies.clear();
	}

	public List<Column> getDependencies(List<Column> cols) { // Get all unique dependencies of the specified columns (expand dependence tree nodes)
		List<Column> ret = new ArrayList<Column>();
		for(Column col : cols) {
			List<Column> deps = col.getDependencies();
			for(Column d : deps) {
				if(!ret.contains(d)) ret.add(d);
			}
		}
		return ret;
	}

	public boolean isStartingColumn() { // True if this column has no dependencies (e.g., constant expression) or is free (user, non-derived) column
		if(!this.isDerived()) {
			return true;
		}
		else if(this.dependencies.isEmpty()) {
			return true;
		}
		else {
			return false;
		}
	}

	public BistroError getDependenceError() { // =canEvaluate. Return one error in the dependencies (recursively) including cyclic dependency error
		for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) { // Loop on expansion layers of dependencies
			for(Column dep : deps) {
				if(dep == this) {
					return new BistroError(BistroErrorCode.DEPENDENCY_CYCLE_ERROR, "Cyclic dependency.", "This column formula depends on itself directly or indirectly.");
				}
				BistroError de = dep.getTranslateError();
				if(de != null && de.code != BistroErrorCode.NONE) {
					return new BistroError(BistroErrorCode.TRANSLATE_PROPAGATION_ERROR, "Error in column " + dep.getName(), "This column formula depends on a column with errors.");
				}
			}
		}
		return null;
	}
	public BistroError getThisOrDependenceError() {
		BistroError ret = this.getTranslateError();
		if(ret != null && ret.code != BistroErrorCode.NONE) {
			return ret; // Error in this column
		}
		return this.getDependenceError();
	}

	public boolean isDependenceDirty() { // =needEvaluate. True if one of the dependencies (recursively) is dirty (formula change)
		for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) { // Loop on expansion layers of dependencies
			for(Column dep : deps) {
				if(dep == this) {
					return true; // Cyclic dependency is also an error and hence dirty
				}
				BistroError de = dep.getTranslateError();
				if(de != null && de.code != BistroErrorCode.NONE) {					return true; // Any error must be treated as dirty status (propagated further down)
				}
				if(dep.isFormulaDirty()) return true;
			}
		}
		return false; // All dependencies are up-to-date
	}
	public boolean isThisOrDependenceDirty() {
		return this.isFormulaDirty() || this.isDependenceDirty();
	}

	//
	// Translation status
	// Translation errors are produced and stored in different objects like many evaluators or local fields (e.g., for links) so the final status is collected
	//
	
	private List<BistroError> translateErrors = new ArrayList<BistroError>();
	public List<BistroError> getTranslateErrors() { // Empty list in the case of no errors
		return this.translateErrors;
	}
	public BistroError getTranslateError() { // Get single (the first) error (there could be many errors detected)
		List<BistroError> ret = this.getTranslateErrors();
		if(ret == null || ret.size() == 0) return null;
		return ret.get(0);
	}
	public boolean hasTranslateErrors() { // Is successfully translated and can be used for evaluation
		if(this.translateErrors.size() == 0) return false;
		else return true;
	}

	//
	// Translate
	//
	
	ColumnEvaluatorCalc evaluatorCalc;
	public void setEvaluatorCalc(ColumnEvaluatorCalc eval) { this.evaluatorCalc = eval; this.expressionKind = ExpressionKind.NONE; }
	ColumnEvaluatorLink evaluatorLink;
	public void setEvaluatorLink(ColumnEvaluatorLink eval) { this.evaluatorLink = eval; this.expressionKind = ExpressionKind.NONE; }
	ColumnEvaluatorAccu evaluatorAccu;
	public void setEvaluatorAccu(ColumnEvaluatorAccu eval) { this.evaluatorAccu = eval; this.expressionKind = ExpressionKind.NONE; }

	// Generate Evaluator* from ColumnDefinition*
	// TODO: What if Evaluator* is provided directly without Formulas/Definition?
	// - setEvaluator means direct, setFormula means translation
	public void translate() {

		this.translateErrors.clear();
		this.resetDependencies();

		List<Column> columns = new ArrayList<Column>();
		
		// Translate depending on the formula kind
		if(this.kind == ColumnKind.CALC) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorCalc = (ColumnEvaluatorCalc) this.definitionCalc.translate(this);
				this.translateErrors.addAll(definitionCalc.getErrors());
			}
			if(this.evaluatorCalc != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorCalc.getDependencies()); // Dependencies
			}
		}
		else if(this.kind == ColumnKind.LINK) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorLink = (ColumnEvaluatorLink) this.definitionLink.translate(this);
				this.translateErrors.addAll(definitionLink.getErrors());
			}
			if(this.evaluatorLink != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorLink.getDependencies()); // Dependencies
			}
		}
		else if(this.kind == ColumnKind.ACCU) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorAccu = (ColumnEvaluatorAccu) this.definitionAccu.translate(this);
				this.translateErrors.addAll(definitionAccu.getErrors());
			}
			if(this.evaluatorAccu != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorAccu.getDependencies()); // Dependencies
			}
		}

		this.setDependencies(columns);
	}

	//
	// Evaluation status
	// Translation errors are produced and stored in different objects like many evaluators or local fields (e.g., for links) so the final status is collected
	//

	private List<BistroError> evaluateErrors = new ArrayList<BistroError>();
	public List<BistroError> getEvaluateErrors() { // Empty list in the case of no errors
		return this.evaluateErrors;
	}
	public BistroError getEvaluateError() { // Get single (the first) error (there could be many errors detected)
		if(this.evaluateErrors == null || this.evaluateErrors.size() == 0) return null;
		return this.evaluateErrors.get(0);
	}
	public boolean hasEvaluateErrors() { // Is successfully evaluated
		if(getEvaluateErrors().size() == 0) return false;
		else return true;
	}

	//
	// Evaluate column
	//

	public void evaluate() {
		this.evaluateErrors.clear();
		
		if(this.getKind() == ColumnKind.CALC) {
			this.evaluatorCalc.evaluate();
			this.evaluateErrors.addAll(this.evaluatorCalc.getErrors());
			if(this.hasEvaluateErrors()) return;
		}
		else if(this.getKind() == ColumnKind.LINK) {
			this.evaluatorLink.evaluate();
			this.evaluateErrors.addAll(this.evaluatorLink.getErrors());
			if(this.hasEvaluateErrors()) return;
		}
		else if(this.getKind() == ColumnKind.ACCU) {
			this.evaluatorAccu.evaluate();
			this.evaluateErrors.addAll(this.evaluatorAccu.getErrors());
			if(this.hasEvaluateErrors()) return;
		}

		this.isChanged = false;

		this.setFormulaClean(); // Mark up-to-date if successful
	}

	//
	// Serialization and construction
	//

	@Override
	public String toString() {
		return "[" + getName() + "]: " + input.getName() + " -> " + output.getName();
	}
	
	@Override
	public boolean equals(Object aThat) {
		if (this == aThat) return true;
		if ( !(aThat instanceof Table) ) return false;
		
		Column that = (Column)aThat;
		
		if(!that.getId().toString().equals(id.toString())) return false;
		
		return true;
	}

	public Column(Schema schema, String name, String input, String output) {
		this.schema = schema;
		this.id = UUID.randomUUID();
		this.name = name;
		this.input = schema.getTable(input);
		this.output = schema.getTable(output);
		
		// Formula
		this.kind = ColumnKind.USER;
		this.expressionKind = ExpressionKind.EXP4J; // By default

		// Data
		this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);
	}
}
