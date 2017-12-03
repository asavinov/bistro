package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class Column implements Element {

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
    // Data (protected). These are used from Table only (all columns change their ranges simultaneously) and from users - users add/remove elements via tables.
    //

    protected void add() { this.data.add(); this.isDirty = true; }
	protected void add(long count) { this.data.add(count); this.isDirty = true; }

    protected void remove() { this.data.remove(1); this.isDirty = true; }
    protected void remove(long count) { this.data.remove(count); this.isDirty = true; }

    //
    // Element interface
    //

    @Override
    public Table getTable() {
        return null;
    }

    @Override
    public Column getColumn() {
        return this;
    }

    @Override
    public List<Element> getDependencies() {
        if(this.definition == null) return new ArrayList<>();
        List<Element> deps = this.definition.getDependencies();
        if(deps == null) return new ArrayList<>();
        return deps;
    }
    @Override
    public boolean hasDependency(Element element) {
        for(Element dep : this.getDependencies()) {
            if(dep == element) return true;
            if(dep.hasDependency(element)) return true; // Recursion
        }
        return false;
    }

    @Override
    public List<Element> getDependants() {
        List<Element> cols = schema.getColumns().stream().filter(x -> x.getDependencies().contains(this)).collect(Collectors.<Element>toList());
        List<Element> tabs = schema.getTables().stream().filter(x -> x.getDependencies().contains(this)).collect(Collectors.<Element>toList());

        List<Element> ret = new ArrayList<>();
        ret.addAll(cols);
        for(Element d : tabs) {
            if(!ret.contains(d)) ret.add(d);
        }

        return ret;
    }
    @Override
    public boolean hasDependant(Element element) {
        for(Element dep : this.getDependants()) {
            if(dep == element) return true;
            if(dep.hasDependant(element)) return true;// Recursion
        }
        return false;
    }

    private List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getDefinitionErrors() { // Empty list in the case of no errors
        return this.definitionErrors;
    }

    @Override
    public boolean hasDefinitionErrorsDeep() { // Recursively
        if(this.definitionErrors.size() > 0) return true; // Check this element

        // Otherwise check errors in dependencies (recursively)
        for(Element dep : this.getDependencies()) {
            if(dep.hasDefinitionErrorsDeep()) return true;
        }

        return false;
    }

    private List<BistroError> executionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getExecutionErrors() { // Empty list in the case of no errors
        return this.executionErrors;
    }

    @Override
    public boolean hasExecutionErrorsDeep() {
        if(executionErrors.size() > 0) return true; // Check this element

        // Otherwise check errors in dependencies (recursively)
        for(Element dep : this.getDependencies()) {
            if(dep.hasExecutionErrorsDeep()) return true;
        }

        return false;
    }

    private boolean isDirty = false;
    @Override
    public boolean isDirty() {
        return this.isDirty;
    }
    @Override
    public void setDirty() {
        this.isDirty = true;
    }

    @Override
    public boolean isDirtyDeep() {
        if(this.isDirty) return true;

        // Otherwise check if there is a dirty dependency (recursively)
        for(Element dep : this.getDependencies()) {
            if(dep.isDirtyDeep()) return true;
        }

        return false;
    }

    @Override
    public void run() {
        this.eval();
    }

    //
    // Evaluate
    //

    // Evaluate only this individual column if possible
    public void eval() {

        // Skip non-derived columns - they do not participate in evaluation
        if(!this.isDerived()) {

            // Propagate dirty status to all dependants before resting it
            if(this.isDirty()) {
                this.getDependants().forEach(x -> x.setDirty());
            }

            this.isDirty = false;

            return;
        }

        // Clear all evaluation errors before any new evaluation
        this.executionErrors.clear();

        // If there are some definition errors then no possibility to eval (including cycles)
        if(this.hasDefinitionErrorsDeep()) { // this.canEvalute false
            // TODO: Add error: cannot evaluate because of definition error in a dependency
            return;
        }
        // No definition errors - canEvaluate true

        // If everything is up-to-date then there is no need to eval
        if(!this.isDirtyDeep()) { // this.needEvaluate false
            // TODO: Add error: cannot evaluate because of dirty dependency
            return;
        }
        // There exists dirty status - needEvaluate true

        // If there were evaluation errors
        if(this.hasExecutionErrorsDeep()) { // this.canEvaluate false
            // TODO: Add error: cannot evaluate because of execution error in a dependency
            return;
        }
        // No errors while evaluating dependencies

        //
        // Really evaluate using definition
        //
        this.definition.eval();

        this.executionErrors.addAll(this.definition.getErrors());

        if(this.executionErrors.size() == 0) {
            this.isDirty = false; // Clean the state (remove dirty flag)
        }
        else {
            this.isDirty = true; // Evaluation failed
        }
    }

    //
    // Column (definition) kind
    //

    ColumnDefinition definition; // It is instantiated by calc-link-accu methods (or definition errors are added)

    protected ColumnDefinitionType definitionType;
    public ColumnDefinitionType getDefinitionType() {
        return this.definitionType;
    }
    public void setDefinitionType(ColumnDefinitionType definitionType) {
        this.definitionType = definitionType;
        this.definitionErrors.clear();
        this.executionErrors.clear();
        this.definition = null;
        this.isDirty = true;
    }
    public boolean isDerived() {
        if(this.definitionType == ColumnDefinitionType.CALC || this.definitionType == ColumnDefinitionType.LINK || this.definitionType == ColumnDefinitionType.ACCU) {
            return true;
        }
        return false;
    }

    //
    // Noop column
    //

    public void noop() {
        this.setDefinitionType(ColumnDefinitionType.NOOP); // Reset definition
    }

    //
    // Key column
    //

    public void key() {
        this.setDefinitionType(ColumnDefinitionType.KEY); // Reset definition
    }

    //
    // Calcuate column
    //

    // Lambda + parameters
    public void calc(Evaluator lambda, ColumnPath... params) { // Specify lambda and parameter valuePaths
        this.setDefinitionType(ColumnDefinitionType.CALC);

        this.definition = new ColumnDefinitionCalc(this, lambda, params); // Create definition
        // TODO: Proces errors. Add excpeitons to the declaration of creator

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    // Lambda + parameters
    public void calc(Evaluator lambda, Column... params) { // Specify lambda and parameter columns
        this.setDefinitionType(ColumnDefinitionType.CALC);

        this.definition = new ColumnDefinitionCalc(this, lambda, params); // Create definition
        // TODO: Proces errors. Add excpeitons to the declaration of creator

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    // Expression
    public void calc(Expression expr) {
        this.setDefinitionType(ColumnDefinitionType.CALC);

        this.definition = new ColumnDefinitionCalc(this, (Expression) expr);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Link column
    //

    // Equality
    public void link(ColumnPath[] valuePaths, Column... keyColumns) {
        this.setDefinitionType(ColumnDefinitionType.LINK);

        this.definition = new ColumnDefinitionLinkPaths(this, valuePaths, keyColumns);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Equality
    public void link(Column[] valueColumns, Column... keyColumns) {
        this.setDefinitionType(ColumnDefinitionType.LINK);

        this.definition = new ColumnDefinitionLinkPaths(this, valueColumns, keyColumns);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Expressions
    public void link(Expression[] valueExprs, Column... keyColumns) { // Custom rhs UDEs for each lhs column
        this.setDefinitionType(ColumnDefinitionType.LINK);

        this.definition = new ColumnDefinitionLinkExprs(this, valueExprs, keyColumns);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Proj column
    //

    // Equality
    public void proj(ColumnPath[] valuePaths, Column... keyColumns) {
        this.setDefinitionType(ColumnDefinitionType.PROJ);

        this.definition = new ColumnDefinitionLinkPaths(this, valuePaths, keyColumns);
        // Output table cannot be noop-table (must be prod-table)
        if(this.getOutput().getDefinitionType() == TableDefinitionType.NOOP) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column definition error.", "Proj-column cannot have noop-table as type - use link-column instead."));
        }

        // Check that all specified key columns are keys of the type table
        Column nonKeyColumn = null;
        for(Column col : keyColumns) {
            if(col.getDefinitionType() != ColumnDefinitionType.KEY) {
                nonKeyColumn = col;
                break;
            }
        }
        if(nonKeyColumn != null) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column definition error.", "All key columns of the proj-column definition must be key columns of the linked product table."));
        }

        // This flag can be important for dependencies
        ((ColumnDefinitionLinkPaths)this.definition).isProj = true;

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Accumulate column
    //

    // Evaluator + parameters OR Expression + no params
    public void accu(ColumnPath accuPath, Evaluator lambda, ColumnPath... params) {
        this.setDefinitionType(ColumnDefinitionType.ACCU);

        Expression accuExpr;
        if(lambda instanceof Expression)
            accuExpr = (Expression)lambda;
        else
            accuExpr = new Expr(lambda, params);

        this.definition = new ColumnDefinitionAccu(this, accuPath, accuExpr);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Evaluator + parameters OR Expression + no params
    public void accu(Column accuPath, Evaluator lambda, Column... params) {
        this.setDefinitionType(ColumnDefinitionType.ACCU);

        Expression accuExpr;
        if(lambda instanceof Expression)
            accuExpr = (Expression)lambda;
        else
            accuExpr = new Expr(lambda, params);

        this.definition = new ColumnDefinitionAccu(this, new ColumnPath(accuPath), accuExpr);

        if(this.hasDependency(this)) {
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
		if ( !(aThat instanceof Column) ) return false;
		
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
		this.definitionType = ColumnDefinitionType.NOOP;

		// Data
		this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);
	}
}
