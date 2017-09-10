package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.expr.*;

import java.lang.reflect.Constructor;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

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
	public void setOutput(Table table) { this.output = table; this.setValue(null); }

	private boolean key = false;
	public boolean isKey() {  return this.key; }

    //
	// Data (public)
	//

    private ColumnData data;

    public Object getValue(long id) { return this.data.getValue(id); }

    public void setValue(long id, Object value) { this.data.setValue(id, value); this.isDirty = true; }

    public void setValue(Object value) { this.data.setValue(value); this.isDirty = true; }

    //
    // Data (protected). These are used from Table only (all columns change their ranges simultaniously) and from users - users add/remove elements via tables.
    //

    protected void add() { this.data.add(); this.isDirty = true; }
	protected void add(long count) { this.data.add(count); this.isDirty = true; }

    protected void remove() { this.data.remove(1); this.isDirty = true; }
    protected void remove(long count) { this.data.remove(count); this.isDirty = true; }

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
    // Data dirty state (~hasDirtyDeep)
    //
    // 0) for USER columns (!isDerived) is defined and interpreted by the user -> USER columns do not participate in dependencies/evaluation, so since USER columns are ignored by evaluate procedure - isDirty is also ignored.

    // 1) add/remove ids in this input (input set population changes) -> what about dependencies?
    // 2) set this output values (this function changes) -> or any of its dependencies recursively
    // 3) definition change of this -> or any of its dependencies
    // 4) definition error of this -> or any of its dependencies

    private boolean isDirty = false;
    public boolean isDirty() {
        return this.isDirty;
    }
    public void setDirty() {
        this.isDirty = true;
    }

    protected boolean hasDirtyDeep() { // This plus inherited dirty status
        if(this.isDirty) return true;

        // Otherwise check if there is a dirty dependency (recursively)
        for(Column dep : this.getDependencies()) {
            if(dep.hasDirtyDeep()) return true;
        }

        return false;
    }

    //
    // Dependencies
    //

    protected List<Column> getDependencies() {
        if(this.evaluator == null) return new ArrayList<>();
        return this.evaluator.getDependencies();
    }
    // Get all unique dependencies of the specified columns
    protected List<Column> getDependencies(List<Column> cols) {
        List<Column> ret = new ArrayList<>();
        for(Column col : cols) {
            List<Column> deps = col.getDependencies();
            for(Column d : deps) {
                if(!ret.contains(d)) ret.add(d);
            }
        }
        return ret;
    }
    // True if this column has no dependencies (e.g., constant expression) or is free (user, non-derived) column
    protected boolean hasDependencies() {
        return !this.getDependencies().isEmpty();
    }


    // Get all columns that depend on this column (but may depend also on other columns)
    protected List<Column> getDependants() {
        List<Column> res = schema.getColumns().stream().filter(x -> x.getDependencies().contains(this)).collect(Collectors.<Column>toList());
        return res;
    }
/*
    // Get columns which have all their dependencies covered by (subset of) the specified list
    protected List<Column> getNextColumns(List<Column> previousColumns) {
        List<Column> ret = new ArrayList<>();

        for(Column col : this.columns) {

            if(previousColumns.contains(col)) continue; // Already in the list. Ccan it really happen without cycles?
            List<Column> deps = col.getDependencies();
            if(deps == null) continue; // Something wrong

            if(previousColumns.containsAll(deps)) { // All column dependencies are in the list
                ret.add(col);
            }
        }

        return ret;
    }
*/

    // Checks if this column depends on itself
    protected boolean isInCyle() {
        for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) {
            for(Column dep : deps) {
                if(!dep.isDerived()) continue;
                if(dep == this) {
                    return true;
                }
            }
        }
        return false;
    }

    //
    // Execution errors (produced after each new evaluation)
    //

    private List<BistroError> evaluationErrors = new ArrayList<>();
    public List<BistroError> getEvaluationErrors() { // Empty list in the case of no errors
        return this.evaluationErrors;
    }

    protected boolean hasEvaluationErrorsDeep() {
        if(evaluationErrors.size() > 1) return true; // Check this column

        // Otherwise check evaluationErrors in dependencies (recursively)
        for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) {
            for(Column dep : deps) {
                if(dep.hasEvaluationErrorsDeep()) return true;
            }
        }

        return false;
    }

    //
    // Evaluate
    //

    ColumnEvaluator evaluator; // It is instantiated by cal-link-accu methods (or translate errors are added)

    // The strategy is to start from the goal (this column), recursively evaluate all dependencies and finally evaluate this column
    public void evaluate() {

        // Skip non-derived columns - they do not participate in evaluation
        if(!this.isDerived()) {
            if(this.isDirty()) {
                this.getDependants().forEach(x -> x.setDirty());
            }
            this.isDirty = false;
            return;
        }

        // Clear all evaluation errors before any new evaluation
        this.evaluationErrors.clear();

        // If there are some definition errors then no possibility to evaluate (including cycles)
        if(this.hasDefinitionErrorsDeep()) { // this.canEvalute false
            return;
        }
        // No definition errors - canEvaluate true

        // If everything is up-to-date then there is no need to evaluate
        if(!this.hasDirtyDeep()) { // this.needEvaluate false
            return;
        }
        // There exists dirty status - needEvaluate true

        // Evaluate dependencies recursively
        for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) {
            for(Column dep : deps) {
                dep.evaluate(); // Whether it is really evaluated depends on the need (dirty status etc.)
            }
        }

        // If there were some evaluation errors
        if(this.hasEvaluationErrorsDeep()) { // this.canEvaluate false
            return;
        }
        // No errors while evaluating dependencies

        // All dependencies are ok so this column can be evaluated
        this.evaluator.evaluate();

        this.evaluationErrors.addAll(this.evaluator.getErrors());

        if(this.evaluationErrors.size() == 0) {
            this.isDirty = false; // Clean the state (remove dirty flag)
        }
        else {
            this.isDirty = true; // Evaluation failed
        }
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
        this.definitionErrors.clear();
        this.evaluationErrors.clear();
        this.evaluator = null;
        this.isDirty = true;
    }
    public boolean isDerived() {
        if(this.kind == ColumnKind.CALC || this.kind == ColumnKind.ACCU || this.kind == ColumnKind.LINK) {
            return true;
        }
        return false;
    }

    //
    // Definition errors. Produced after each new definition
    //

    private List<BistroError> definitionErrors = new ArrayList<>();
    public List<BistroError> getDefinitionErrors() { // Empty list in the case of no errors
        return this.definitionErrors;
    }

    public boolean hasDefinitionErrorsDeep() { // Recursively
        if(this.definitionErrors.size() > 0) return true; // Check this column

        // Otherwise check errors in dependencies (recursively)
        for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) {
            for(Column dep : deps) {
                if(dep.hasDefinitionErrorsDeep()) return true;
            }
        }

        return false;
    }

    // Patterns:
    // 1) calculate(Ude.class/Ude.name, formula) <- here we provide translator plus proceudre/binding (new instance will be given formula string)
    // 2) calculate(lambda/Ude.class/Ude.name, path_objects/names) <- here we provide procedure plus binding (instance will be given parameter paths)

    //
    // Calcuate. Convert input parameters into an Evaluator object and ealuate it in the case of immediate (eager) action.
    //

    public void calculate(UDE ude) { // Provide instance of custom UDE which has already paths
        this.setKind(ColumnKind.CALC); // Reset definition

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void calculate(String clazz, String formula) { // Specify UDE class/selector and formula parameter for this class
        this.setKind(ColumnKind.CALC); // Reset definition

        if(clazz.equals(UDE.Exp4j)) {

            UDE expr = new UdeExp4j(formula, this.input);

            this.definitionErrors.addAll(expr.getTranslateErrors());
            if(this.definitionErrors.size() > 1) return;

            this.evaluator = new ColumnEvaluatorCalc(this, expr);

            if(this.isInCyle()) {
                this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            }
        }
        else {
            ; // TODO: Error - UDE class not available/implemented
        }
    }

    public void calculate(Class clazz, List<ColumnPath> paths) { // Specify UDE class and parameter paths
        this.setKind(ColumnKind.CALC); // Reset definition

        // Instantiate specified class
        UDE ude = null;
        try {
            Constructor[] ccc = clazz.getConstructors();
            ude = (UDE)clazz.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        ude.setResolvedParamPaths(paths); // Set parameter paths (or use constructor)

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void calculate(UdeEvaluate lambda, List<ColumnPath> paths) { // Specify lambda and parameter paths
        this.setKind(ColumnKind.CALC); // Reset definition

        UDE ude = new UdeLambda(lambda);

        ude.setResolvedParamPaths(paths); // Set parameter paths (or use constructor)

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Link
    //

    public void link(List<Column> columns, List<UDE> udes) { // Custom rhs UDEs for each lhs column
        this.setKind(ColumnKind.LINK); // Reset definition

        this.evaluator = new ColumnEvaluatorLink(this, columns, udes);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void link(String clazz, List<String> names, List<String> formulas) { // Column names in the output table and expressions in the input table
        this.setKind(ColumnKind.LINK); // Reset definition

        if(clazz.equals("EQUAL")) {
            ; // TODO: Implement UdeEqual expression which simply returns value of the specified column
        }
        else if(clazz.equals(UDE.Exp4j)) {
            // Resolve all names
            List<Column> columns = new ArrayList<>();
            for(String name : names) {
                Column col = this.output.getColumn(name);
                if(col == null) {
                    // TODO: Add error
                    return;
                }
                columns.add(col);
            }

            // Translate all formulas
            List<UDE> exprs = new ArrayList<>();
            for(String formula : formulas) {
                UDE expr = new UdeExp4j(formula, this.input);
                this.definitionErrors.addAll(expr.getTranslateErrors());
                if(this.definitionErrors.size() > 1) return;
                exprs.add(expr);
            }

            this.evaluator = new ColumnEvaluatorLink(this, columns, exprs);

            if(this.isInCyle()) {
                this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            }
        }
        else {
            ; // TODO: Error - UDE class not available/implemented
        }
    }

    //
    // Accumulate
    //

    public void accumulate(UDE initUde, UDE accuUde, UDE finUde, ColumnPath accuPath) { // Provide instance of custom UDEs which have already paths
        this.setKind(ColumnKind.ACCU); // Reset definition

        this.evaluator = new ColumnEvaluatorAccu(this, initUde, accuUde, finUde, accuPath);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void accumulate(String clazz, String initFormula, String accuFormula, String finFormula, String accuTableName, NamePath accuLinkPath) { // Specify UDE class/selector and formulas
        this.setKind(ColumnKind.ACCU); // Reset definition

        if(clazz.equals(UDE.Exp4j)) {

            ColumnPath accuPathColumns = null;

            // Accu table and link (group) path
            Table accuTable = schema.getTable(accuTableName);
            if(accuTable == null) { // Binding error
                this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Binding error.", "Cannot find table: " + accuTableName));
                return;
            }
            accuPathColumns = accuLinkPath.resolveColumns(accuTable);
            if(accuPathColumns == null) { // Binding error
                this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Binding error.", "Cannot find columns: " + accuLinkPath.toString()));
                return;
            }

            UDE initExpr = null;

            // Initialization (always initialize - even for empty formula)
            if(initFormula == null || initFormula.isEmpty()) { // TODO: We need UDE for constants and for equality (equal to the specified column)
                initExpr = new UdeExp4j(this.getDefaultValue().toString(), this.input);
            }
            else {
                initExpr = new UdeExp4j(initFormula, this.input);
            }
            this.definitionErrors.addAll(initExpr.getTranslateErrors());
            if(this.definitionErrors.size() > 1) return; // Cannot proceed

            // Accumulation
            UDE accuExpr = null;
            accuExpr = new UdeExp4j(accuFormula, accuTable);
            this.definitionErrors.addAll(accuExpr.getTranslateErrors());
            if(this.definitionErrors.size() > 1) return; // Cannot proceed

            // Finalization
            UDE finExpr = null;
            if(finFormula != null && !finFormula.isEmpty()) {
                finExpr = new UdeExp4j(finFormula, this.input);
                this.definitionErrors.addAll(finExpr.getTranslateErrors());
                if(this.definitionErrors.size() > 1) return; // Cannot proceed
            }

            // Errors
            if(initExpr == null || accuExpr  == null /* || finExpr == null */) { // TODO: finExpr can be null in the case of no formula. We need to fix this and distinguis between errors and having no formula.
                String frml = "";
                if(initExpr == null) frml = initFormula;
                else if(accuExpr == null) frml = accuFormula;
                else if(finExpr == null) frml = finFormula;
                this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot create expression. " + frml));
                return;
            }

            this.definitionErrors.addAll(initExpr.getTranslateErrors());
            this.definitionErrors.addAll(accuExpr.getTranslateErrors());
            // this.definitionErrors.addAll(finExpr.getTranslateErrors()); // TODO: Fix uncertainty with null expression in the case of no formula and in the case of errors
            if(this.definitionErrors.size() > 1) return; // Cannot proceed

            this.evaluator = new ColumnEvaluatorAccu(this, initExpr, accuExpr, finExpr, accuPathColumns);

            if(this.isInCyle()) {
                this.definitionErrors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            }
        }
        else {
            ; // TODO: Error - UDE class not available/implemented
        }
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

	public Column(Schema schema, String name, Table input, Table output) {
		this.schema = schema;
		this.id = UUID.randomUUID();
		this.name = name;
		this.input = input;
		this.output = output;
		
		// Formula
		this.kind = ColumnKind.NONE;

		// Data
		this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);
	}
}
