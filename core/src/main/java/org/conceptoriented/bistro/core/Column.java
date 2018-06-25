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
    // Change status
    //

    protected long changedAt; // Time of latest change
    @Override
    public long getChangedAt() {
        return this.changedAt;
    }

    private boolean isChanged = false;
    @Override
    public boolean isChanged() {
        return this.isChanged;
    }

    @Override
    public void setChanged() {
        this.isChanged = true;
        this.changedAt = System.nanoTime();
    }

    @Override
    public void resetChanged() { // Forget about the change status/scope (reset change delta)
        this.isChanged = false;
    }

    @Override
    public boolean isDirty() {

        // Definition has changed
        if(this.operation != null) {
            if(this.getDefinitionChangedAt() > this.getChangedAt()) return true;
        }

        // One of its dependencies has changes or is dirty
        for(Element dep : this.getDependencies()) {
            if(dep.getChangedAt() > this.getChangedAt()) return true;
            if(dep.isDirty()) return true; // Recursion
        }

        return false;
    }

    //
    // Data (public)
    //

    private ColumnData data;

    public Object getValue(long id) { return this.data.getValue(id); }

    // Note: we do not set the change flag by assuming that only newly added records are changed - if it is not so then it has to be set manually
    // Note: methods are not safe - they do not check the validity of arguments (ids, values etc.)

    // One id
    public void setValue(long id, Object value) { this.data.setValue(id, value); }

    // Range of ids
    public void setValue(Range range, Object value) { this.data.setValue(range, value); }
    public void setValue(Range range) { this.data.setValue(range); } // Default value

    // All ids
    public void setValue(Object value) { this.data.setValue(value); this.setChanged(); }
    public void setValue() { this.data.setValue(); this.setChanged(); } // Default value

    // Default value
    public Object getDefaultValue() { return this.data.getDefaultValue(); }
    public void setDefaultValue(Object value) { this.data.setDefaultValue(value); this.setChanged(); }

    //
    // Data (protected). They are used from Table only and determine physically existing mapping from a range of ids to output values.
    //

    protected void add() { this.data.add(); this.isChanged = true; }
    protected void add(long count) { this.data.add(count); this.isChanged = true; }

    protected void remove() { this.data.remove(1); this.isChanged = true; }
    protected void remove(long count) { this.data.remove(count); this.isChanged = true; } // Remove the specified number of oldest records
    protected void removeAll() { this.data.removeAll(); this.isChanged = true; }

    protected void reset() {
        Table table = this.getInput();
        if(table != null) {
            this.data.reset(table.getIdRange().start, table.getIdRange().end);
        }
        else {
            this.data.reset(0, 0);
        }
        this.isChanged = true;
    }

    protected long findSorted(Object value) { return this.data.findSorted(value); }
    protected long findSortedFromStart(Object value) { return this.data.findSortedFromStart(value); }

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
        List<Element> deps = new ArrayList<>();

        if(this.getOperationType() == OperationType.NOOP) {
            if(this.isKey() && this.getInput().getOperationType() == OperationType.PRODUCT) {
                deps.add(this.getInput()); // Key-columns depend on the product-table (if any) because they are filled by their population procedure
            }
        }
        else if(this.operation != null) {
            deps = this.operation.getDependencies();
            if(deps == null) deps = new ArrayList<>();
        }

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
    public List<Element> getDependents() {
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
    public boolean hasDependents(Element element) {
        for(Element dep : this.getDependents()) {
            if(dep == element) return true;
            if(dep.hasDependents(element)) return true;// Recursion
        }
        return false;
    }

    private List<BistroException> errors = new ArrayList<>();
    @Override
    public List<BistroException> getErrors() { // Empty list in the case of no errors
        return this.errors;
    }

    @Override
    public boolean hasErrorsDeep() {
        if(errors.size() > 0) return true; // Check this element

        // Otherwise check errors in dependencies (recursively)
        for(Element dep : this.getDependencies()) {
            if(dep.hasErrorsDeep()) return true;
        }

        return false;
    }

    @Override
    public void evaluate() { // Evaluate only this individual column if possible

        // Skip non-derived elements since they do not participate in evaluation (nothing to evaluate)
        if(!this.isDerived()) {
            return;
        }

        //
        // Check can evaluate
        //

        this.errors.clear();

        if(this.hasErrorsDeep()) {
            // TODO: Add error: cannot evaluate because of execution error in a dependency
            return;
        }

        if(this.operation == null) {
            return;
        }

        //
        // Check need to evaluate
        //

        if(!this.isDirty()) {
            // TODO: Add error: cannot evaluate because of dirty dependency
            return;
        }

        //
        // Really evaluate
        //

        this.operation.evaluate();
        this.errors.addAll(this.operation.getErrors());
    }

    //
    // Column (operation) kind
    //

    Operation operation;

    @Override
    public OperationType getOperationType() {
        if(this.operation == null) return OperationType.NOOP;
        else return this.operation.getOperationType();
    }

    public boolean isDerived() {
        if(this.getOperationType() == OperationType.NOOP) {
            return false;
        }
        return true;
    }

    protected long definitionChangedAt; // Time of latest change
    public long getDefinitionChangedAt() {
        return this.definitionChangedAt;
    }

    //
    // Noop column
    //

    // If true, its outputs will be set by the table population procedure during inference for each new instance
    // It is actually part of the noop-column operation (only noop-columns can be keys)
    private boolean key = false;
    public boolean isKey() {
        return this.key;
    }

    public void noop(boolean isKey) {
        this.errors.clear();

        this.operation = null;
        this.definitionChangedAt = System.nanoTime();

        this.key = isKey;
        // TODO: Error check: some other definitions might become invalid if they depend on the key column status
        //   Either mark this column as having a operation error (so it will skipped), or mark other columns as having operation errors
    }

    //
    // Calcuate column
    //

    public void calculate(EvalCalculate lambda, ColumnPath... paths) {
        this.noop(false);

        this.operation = new OpCalculate(this, lambda, paths);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void calculate(EvalCalculate lambda, Column... columns) {
        this.noop(false);

        this.operation = new OpCalculate(this, lambda, columns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Link column
    //

    public void link(ColumnPath[] valuePaths, Column... keyColumns) {
        this.noop(false);

        this.operation = new OpLink(this, valuePaths, keyColumns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void link(Column[] valueColumns, Column... keyColumns) {
        this.noop(false);

        this.operation = new OpLink(this, valueColumns, keyColumns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void link(ColumnPath valuePath) { // Link to range table (using inequality as a condition)
        this.noop(false);

        this.operation = new OpLink(this, valuePath);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Project to values (using equality)
    //

    public void project(ColumnPath[] valuePaths, Column... keyColumns) {
        this.noop(false);

        this.operation = new OpProject(this, valuePaths, keyColumns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void project(Column[] valueColumns, Column... keyColumns) {
        this.noop(false);

        this.operation = new OpProject(this, valueColumns, keyColumns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Project to ranges/intervals (using inequality)
    //

    public void project(ColumnPath valuePath) {
        this.noop(false);

        this.operation = new OpProject(this, valuePath);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void project(Column valueColumn) {
        this.noop(false);

        this.operation = new OpProject(this, new ColumnPath(valueColumn));

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Accumulate column
    //

    public void accumulate(ColumnPath groupPath, EvalAccumulate adder, EvalAccumulate remover, ColumnPath... paths) {
        this.noop(false);

        this.operation = new OpAccumulate(this, groupPath, adder, remover, paths);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void accumulate(Column groupColumn, EvalAccumulate adder, EvalAccumulate remover, Column... columns) {
        this.noop(false);

        this.operation = new OpAccumulate(this, groupColumn, adder, remover, columns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Rolling column
    //

    public void roll(int sizePast, int sizeFuture, EvalRoll lambda, ColumnPath... paths) {
        this.noop(false);

        this.operation = new OpRoll(this, null, sizePast, sizeFuture, lambda, paths);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void roll(int sizePast, int sizeFuture, EvalRoll lambda, Column... columns) {
        this.noop(false);

        this.operation = new OpRoll(this, null, sizePast, sizeFuture, lambda, columns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void roll(ColumnPath distancePath, int sizePast, int sizeFuture, EvalRoll lambda, ColumnPath... paths) {
        this.noop(false);

        this.operation = new OpRoll(this, distancePath, sizePast, sizeFuture, lambda, paths);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void roll(Column distanceColumn, int sizePast, int sizeFuture, EvalRoll lambda, Column... columns) {
        this.noop(false);

        this.operation = new OpRoll(this, distanceColumn, sizePast, sizeFuture, lambda, columns);

        if(this.hasDependency(this)) {
            this.noop(false);
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Serialization and construction
    //

    @Override
    public String toString() {
        return "[" + getName() + "]: " + input.getName() + " -> " + this.getOperationType() + " -> " + output.getName();
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

        // Where its output values are stored
        this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);

        this.changedAt = 0; // Very old - need to be evaluated
    }
}
