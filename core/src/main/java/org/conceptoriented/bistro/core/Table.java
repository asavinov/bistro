package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.operations.OpProduct;
import org.conceptoriented.bistro.core.operations.OpRange;

import java.util.*;

public class Table implements Element {
    private Schema schema;
    public Schema getSchema() {
        return schema;
    }

    private final UUID id;
    public UUID getId() {
        return id;
    }

    private String name;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    private static List<String> primitiveNames = Arrays.asList("Object", "Double", "Integer", "String");
    public boolean isPrimitive() {
        return this.primitiveNames.stream().anyMatch(x -> x.equalsIgnoreCase(this.name));
    }

    public List<Column> getColumns() {
        return this.schema.getColumns(this);
    }
    public Column getColumn(String name) {
        return this.schema.getColumn(this, name);
    }

    //
    // Data
    //

    private TableData data;
    public TableData getData() {
        return this.data;
    }
    public void setData(TableData data) {
        this.data = data;
    }

    //
    // Element interface
    //

    @Override
    public Table getTable() {
        return this;
    }

    @Override
    public Column getColumn() {
        return null;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> deps = new ArrayList<>();

        if(this.getOperationType() == OperationType.NOOP) {
            return deps;
        }

        if(this.operation != null) {
            deps = this.operation.getDependencies();
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
        return new ArrayList<>();
    }
    @Override
    public boolean hasDependents(Element element) {
        for(Element dep : this.getDependents()) {
            if(dep == this) return true;
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
    public boolean isDirty() {

        // Definition has changed
        if(this.getDefinitionChangedAt() > this.getData().getChangedAt()) return true;

        // One of its dependencies has changes or is dirty
        long thisChangedAt = this.getData().getChangedAt();
        for(Element dep : this.getDependencies()) {
            if(dep instanceof Column) {
                if(((Column)dep).getData().getChangedAt()  > thisChangedAt) return true;
            }
            else if(dep instanceof Table) {
                if(((Table)dep).getData().getChangedAt()  > thisChangedAt) return true;
            }

            if(dep.isDirty()) return true; // Recursion
        }

        return false;
    }

    //
    // Evaluate
    //

    @Override
    public void evaluate() {

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

        if(this.getOperationType() == OperationType.NOOP) {
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
        // Check if it is a project table (if it is populated by some incoming project column)
        //

        boolean isProj = false;
        for(Column col : this.schema.getColumns()) {
            if(col.getOperationType() != OperationType.PROJECT) continue;
            isProj = true;
            break;
        }

        if(isProj) {
            // Only reset to initial state (empty). Population will be performed by project columns
            this.getData().reset();
            return;
        }

        //
        // Populate using own definition
        //

        try {
            this.operation.evaluate();
        }
        catch(BistroException e) {
            this.errors.add(e);
        }
        catch(Exception e) {
            this.errors.add( new BistroException(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "Error populating table.") );
        }
    }

    //
    // Table (operation) kind
    //

    Operation operation;
    @Override
    public Operation getOperation() {
        return this.operation;
    }
    @Override
    public void setOperation(Operation operation) {
        this.errors.clear();
        this.definitionChangedAt = System.nanoTime();

        this.operation = operation;

        if(this.hasDependency(this)) {
            this.noop(); // Reset definition because of failure to set new operation
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This table depends on itself directly or indirectly.");
        }
    }

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
    // Noop table
    //

    public void noop() {
        Operation op = null;
        this.setOperation(op);

        // TODO: Should we reset/empty the data or retain its current population?
    }

    //
    // Product table
    //

    public void product() {
        Operation op = new OpProduct(this);
        this.setOperation(op);
    }

    public void product(EvalCalculate lambda, ColumnPath... paths) {
        Operation op = new OpProduct(this, lambda, paths);
        this.setOperation(op);
    }

    public void product(EvalCalculate lambda, Column... columns) {
        Operation op = new OpProduct(this, lambda, columns);
        this.setOperation(op);
    }

    //
    // Range table
    //

    public void range(Object origin, Object period, Long length) {
        Operation op = new OpRange(this, origin, period, length);
        this.setOperation(op);
    }

    //
    // Convenience methods
    //

    public List<Column> getKeyColumns() { // Get all columns the domains of which have to be combined (non-primitive key-columns)
        List<Column> ret = new ArrayList<>();
        for(Column col : this.getColumns()) {
            if(!col.isKey()) continue; // Skip non-key columns
            ret.add(col);
        }
        return ret;
    }

    public List<Column> getProjColumns() { // Get all incoming project-columns
        List<Column> ret = new ArrayList<>();
        for(Column col : this.getSchema().getColumns()) {
            if(col.getOutput() != this) continue;
            if(col.getOperationType() != OperationType.PROJECT) continue; // Skip non-key columns
            ret.add(col);
        }
        return ret;
    }

    //
    // Serialization and construction
    //

    @Override
    public String toString() {
        return "[" + name + "] - " + this.getOperationType();
    }

    @Override
    public boolean equals(Object aThat) {
        if (this == aThat) return true;
        if ( !(aThat instanceof Table) ) return false;

        Table that = (Table)aThat;

        if(!that.getId().toString().equals(id.toString())) return false;

        return true;
    }

    public Table(Schema schema, String name) {
        this.schema = schema;
        this.id = UUID.randomUUID();
        this.name = name;

        this.data = new org.conceptoriented.bistro.core.data.TableDataImpl(this);
    }
}
