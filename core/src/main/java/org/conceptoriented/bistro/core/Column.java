package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.expr.*;

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

    public void setValue() { this.data.setValue(); this.isDirty = true; }

    //
    // Data (protected). These are used from Table only (all columns change their ranges simultaniously) and from users - users add/remove elements via tables.
    //

    protected void add() { this.data.add(); this.isDirty = true; }
	protected void add(long count) { this.data.add(count); this.isDirty = true; }

    protected void remove() { this.data.remove(1); this.isDirty = true; }
    protected void remove(long count) { this.data.remove(count); this.isDirty = true; }

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
        List<Column> deps = this.evaluator.getDependencies();
        if(deps == null) return new ArrayList<>();
        return deps;
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

    // Get all columns that (directly) depend on this column
    protected List<Column> getDependants() {
        List<Column> res = schema.getColumns().stream().filter(x -> x.getDependencies().contains(this)).collect(Collectors.<Column>toList());
        return res;
    }

    // Checks if this column depends on itself
    protected boolean isInCyle() {
        for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) {
            for(Column dep : deps) {
                if(dep == this) {
                    return true;
                }
            }
        }
        return false;
    }

    //
    // Execution errors (cleaned, and then produced after each new evaluation)
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
                if(dep == this) return true;
                if(dep.getEvaluationErrors().size() > 0) return true;
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
    // Formula definitionType
    //

    protected DefinitionType definitionType;
    public DefinitionType getDefinitionType() {
        return this.definitionType;
    }
    public void setDefinitionType(DefinitionType definitionType) {
        this.definitionType = definitionType;
        this.definitionErrors.clear();
        this.evaluationErrors.clear();
        this.evaluator = null;
        this.isDirty = true;
    }
    public boolean isDerived() {
        if(this.definitionType == DefinitionType.CALC || this.definitionType == DefinitionType.ACCU || this.definitionType == DefinitionType.LINK) {
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
                if(dep == this) return true;
                if(dep.getDefinitionErrors().size() > 0) return true;
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
        this.setDefinitionType(DefinitionType.CALC); // Reset definition

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void calculate(String clazz, String formula) { // Specify UDE class/selector and formula parameter for this class
        this.setDefinitionType(DefinitionType.CALC); // Reset definition

        UDE ude = null;
        try {
            ude = UdeFormula.createInstance(clazz, formula, this.input);
        }
        catch(BistroError e) {
            this.definitionErrors.add(e);
            return;
        }

        this.evaluator = new ColumnEvaluatorCalc(this, ude);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void calculate(Class clazz, List<ColumnPath> paths) { // Specify UDE class and parameter paths
        this.setDefinitionType(DefinitionType.CALC); // Reset definition

        // Instantiate specified class
        UDE ude = null;
        try {
            ude = (UDE)clazz.newInstance();
        } catch (InstantiationException e) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Expression creation error.", "Cannot create instance of the specified expression class.", e));
            return;
        } catch (IllegalAccessException e) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Expression creation error.", "Cannot create instance of the specified expression class.", e));
            return;
        }

        ude.setResolvedParamPaths(paths); // Set parameter paths (or use constructor)

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void calculate(UdeEvaluate lambda, List<ColumnPath> paths) { // Specify lambda and parameter paths
        this.setDefinitionType(DefinitionType.CALC); // Reset definition

        UDE ude = null;
        try {
            ude = UdeLambda.createInstance(lambda);
        }
        catch(BistroError e) {
            this.definitionErrors.add(e);
            return;
        }

        ude.setResolvedParamPaths(paths); // Set parameter paths (or use constructor)

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    //
    // Link
    //

    public void link(List<Column> columns, List<UDE> udes) { // Custom rhs UDEs for each lhs column
        this.setDefinitionType(DefinitionType.LINK); // Reset definition

        this.evaluator = new ColumnEvaluatorLink(this, columns, udes);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void link(String clazz, List<String> names, List<String> formulas) { // Column names in the output table and expressions in the input table
        this.setDefinitionType(DefinitionType.LINK); // Reset definition

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
        List<UDE> udes = new ArrayList<>();
        for(String formula : formulas) {
            UdeFormula expr = null;
            try {
                expr = UdeFormula.createInstance(clazz, formula, this.input);
            }
            catch(BistroError e) {
                this.definitionErrors.add(e);
                return;
            }
            udes.add(expr);
        }

        this.evaluator = new ColumnEvaluatorLink(this, columns, udes);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Accumulate
    //

    public void accumulate(UDE initUde, UDE accuUde, UDE finUde, ColumnPath accuPath) { // Provide instance of custom UDEs which have already paths
        this.setDefinitionType(DefinitionType.ACCU); // Reset definition

        this.evaluator = new ColumnEvaluatorAccu(this, initUde, accuUde, finUde, accuPath);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    public void accumulate(String clazz, String initFormula, String accuFormula, String finFormula, String accuTableName, NamePath accuLinkPath) { // Specify UDE class/selector and formulas
        this.setDefinitionType(DefinitionType.ACCU); // Reset definition

        // Accu table and link (group) path
        Table accuTable = schema.getTable(accuTableName);
        if(accuTable == null) { // Binding error
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Binding error.", "Cannot find table: " + accuTableName));
            return;
        }

        // Accu (group) path
        ColumnPath accuPathColumns = accuLinkPath.resolveColumns(accuTable);
        if(accuPathColumns == null) { // Binding error
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Binding error.", "Cannot find columns: " + accuLinkPath.toString()));
            return;
        }

        // Initialization
        UDE initExpr = null;
        try {
            initExpr = UdeFormula.createInstance(clazz, initFormula, this.input);
        }
        catch(BistroError e) {
            this.definitionErrors.add(e);
            return;
        }

        // Accumulation
        UDE accuExpr = null;
        try {
            accuExpr = UdeFormula.createInstance(clazz, accuFormula, accuTable); // Note that we use a different table (not this column input)
        }
        catch(BistroError e) {
            this.definitionErrors.add(e);
            return;
        }

        // Finalization
        UDE finExpr = null;
        if(finFormula != null && !finFormula.isEmpty()) {
            try {
                finExpr = UdeFormula.createInstance(clazz, finFormula, this.input);
            }
            catch(BistroError e) {
                this.definitionErrors.add(e);
                return;
            }
        }

        this.evaluator = new ColumnEvaluatorAccu(this, initExpr, accuExpr, finExpr, accuPathColumns);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
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
		this.definitionType = DefinitionType.NONE;

		// Data
		this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);
	}
}
