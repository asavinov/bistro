package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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

    public Object getDefaultValue() { return this.data.getDefaultValue(); }
    public void setDefaultValue(Object value) { this.data.setDefaultValue(value); this.isDirty = true; }

    //
    // Data (protected). These are used from Table only (all columns change their ranges simultaniously) and from users - users add/remove elements via tables.
    //

    protected void add() { this.data.add(); this.isDirty = true; }
	protected void add(long count) { this.data.add(count); this.isDirty = true; }

    protected void remove() { this.data.remove(1); this.isDirty = true; }
    protected void remove(long count) { this.data.remove(count); this.isDirty = true; }

    //
    // Data dirty state (~hasDirtyDeep)
    //
    // 0) for USER columns (!isDerived) is defined and interpreted by the user -> USER columns do not participate in dependencies/evaluation, so since USER columns are ignored by eval procedure - isDirty is also ignored.

    // 1) add/remove ids in this input (input set population changes) -> what about dependencies?
    // 2) set this output paths (this function changes) -> or any of its dependencies recursively
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

    public List<Column> getDependencies() {
        if(this.definition == null) return new ArrayList<>();
        List<Column> deps = this.definition.getDependencies();
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

    ColumnDefinition definition; // It is instantiated by cal-link-accu methods (or translate errors are added)

    // The strategy is to start from the goal (this column), recursively eval all dependencies and finally eval this column
    public void eval() {

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

        // If there are some definition errors then no possibility to eval (including cycles)
        if(this.hasDefinitionErrorsDeep()) { // this.canEvalute false
            return;
        }
        // No definition errors - canEvaluate true

        // If everything is up-to-date then there is no need to eval
        if(!this.hasDirtyDeep()) { // this.needEvaluate false
            return;
        }
        // There exists dirty status - needEvaluate true

        // Evaluate dependencies recursively
        for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) {
            for(Column dep : deps) {
                dep.eval(); // Whether it is really evaluated depends on the need (dirty status etc.)
            }
        }

        // If there were some evaluation errors
        if(this.hasEvaluationErrorsDeep()) { // this.canEvaluate false
            return;
        }
        // No errors while evaluating dependencies

        // All dependencies are ok so this column can be evaluated
        this.definition.eval();

        this.evaluationErrors.addAll(this.definition.getErrors());

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

    protected ColumnDefinitionType definitionType;
    public ColumnDefinitionType getDefinitionType() {
        return this.definitionType;
    }
    public void setDefinitionType(ColumnDefinitionType definitionType) {
        this.definitionType = definitionType;
        this.definitionErrors.clear();
        this.evaluationErrors.clear();
        this.definition = null;
        this.isDirty = true;
    }
    public boolean isDerived() {
        if(this.definitionType == ColumnDefinitionType.CALC || this.definitionType == ColumnDefinitionType.ACCU || this.definitionType == ColumnDefinitionType.LINK) {
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

    //
    // Calcuate. Convert input parameters into an Evaluator object and ealuate it in the case of immediate (eager) action.
    //

    // Lambda + parameters
    public void calc(Evaluator lambda, ColumnPath... params) { // Specify lambda and parameter paths
        this.setDefinitionType(ColumnDefinitionType.CALC); // Reset definition

        this.definition = new ColumnDefinitionCalc(this, lambda, params); // Create definition
        // TODO: Proces errors. Add excpeitons to the declaration of creator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    // Lambda + parameters
    public void calc(Evaluator lambda, Column... params) { // Specify lambda and parameter columns
        this.setDefinitionType(ColumnDefinitionType.CALC); // Reset definition

        this.definition = new ColumnDefinitionCalc(this, lambda, params); // Create definition
        // TODO: Proces errors. Add excpeitons to the declaration of creator

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    // Expression
    public void calc(Expression expr) {
        this.setDefinitionType(ColumnDefinitionType.CALC); // Reset definition

        this.definition = new ColumnDefinitionCalc(this, (Expression) expr);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Link
    //

    // Equality
    public void link(Column[] columns, ColumnPath... params) {
        this.setDefinitionType(ColumnDefinitionType.LINK); // Reset definition

        this.definition = new ColumnDefinitionLinkPaths(this, columns, params);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Equality
    public void link(Column[] columns, Column... params) {
        this.setDefinitionType(ColumnDefinitionType.LINK); // Reset definition

        this.definition = new ColumnDefinitionLinkPaths(this, columns, params);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Expressions
    public void link(Column[] columns, Expression... exprs) { // Custom rhs UDEs for each lhs column
        this.setDefinitionType(ColumnDefinitionType.LINK); // Reset definition

        this.definition = new ColumnDefinitionLinkExprs(this, columns, exprs);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Accumulate
    //

    // Evaluator + parameters
    public void accu(ColumnPath accuPath, Evaluator lambda, ColumnPath... params) {
        this.setDefinitionType(ColumnDefinitionType.ACCU); // Reset definition

        Expression accuExpr = new Expr(lambda, params);
        this.definition = new ColumnDefinitionAccu(this, accuPath, accuExpr);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Evaluator + parameters
    public void accu(Column accuPath, Evaluator lambda, Column... params) {
        this.setDefinitionType(ColumnDefinitionType.ACCU); // Reset definition

        Expression accuExpr = new Expr(lambda, params);
        this.definition = new ColumnDefinitionAccu(this, new ColumnPath(accuPath), accuExpr);

        if(this.isInCyle()) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Expression
    public void accu(ColumnPath accuPath, Expression expr) { // Provide instance of custom UDEs which have already paths
        this.setDefinitionType(ColumnDefinitionType.ACCU); // Reset definition

        this.definition = new ColumnDefinitionAccu(this, accuPath, expr);

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
		this.definitionType = ColumnDefinitionType.NONE;

		// Data
		this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);
	}
}
