package org.conceptoriented.bistro.core;

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
    // Change status is delta between previous state and current state (as a number of added and removed records)
    //

    protected Range addedRange = new Range(); // Newly added ids
    public Range getAddedRange() {
        return this.addedRange;
    }

    protected Range removedRange = new Range(); // Removed ids
    public Range getRemovedRange() {
        return this.removedRange;
    }

    public Range getIdRange() {
        return new Range(this.removedRange.end, this.addedRange.end); // Derived from added and removed
    }

    public long getLength() {
        return this.addedRange.end - this.removedRange.end; // All non-removed records
    }

    protected long changedAt; // Time of latest change
    @Override
    public long getChangedAt() {
        return this.changedAt;
    }

    @Override
    public boolean isChanged() { // Changes in a table are made by adding and removing records
        if(this.addedRange.getLength() != 0) return true;
        if(this.removedRange.getLength() != 0) return true;
        return false;
    }

    @Override
    public void setChanged() {
        this.changedAt = System.nanoTime();
    }

    @Override
    public void resetChanged() { // Forget about the change status/scope/delta without changing the valid data currently in the tables
        this.addedRange.start = this.addedRange.end;
        this.removedRange.start = this.removedRange.end;
    }

    @Override
    public boolean isDirty() {

        // Definition has changed
        if(this.getDefinitionChangedAt() > this.getChangedAt()) return true;

        // One of its dependencies has changes or is dirty
        for(Element dep : this.getDependencies()) {
            if(dep.getChangedAt() > this.getChangedAt()) return true;
            if(dep.isDirty()) return true; // Recursion
        }

        return false;
    }

    //
    // Operations with table population
    //

    public long add() { // Add new elements with the next largest id. The created id is returned
        this.getColumns().forEach( x -> x.add() );
        this.addedRange.end++; // Grows along with valid ids
        this.changedAt = System.nanoTime();
        return this.addedRange.end - 1; // Return id of the added element
    }

    public Range add(long count) {
        this.getColumns().forEach( x -> x.add(count) );
        this.addedRange.end += count; // Grows along with valid ids
        this.changedAt = System.nanoTime();
        return new Range(this.addedRange.end - count, this.addedRange.end); // Return ids of added elements
    }

    public long remove() { // Remove oldest elements with smallest ids. The removed id is returned.
        this.getColumns().forEach( x -> x.remove() );
        if(this.getLength() > 0) { this.removedRange.end++; this.changedAt = System.nanoTime(); }
        return this.removedRange.end - 1; // Id of the removed record (this id is not valid anymore)
    }

    public Range remove(long count) {
        long toRemove = Math.min(count, this.getLength());
        if(toRemove > 0) { this.removedRange.end += toRemove; this.changedAt = System.nanoTime(); }
        return new Range(this.removedRange.end - toRemove, this.removedRange.end);
    }

    public void removeAll() {
        if(this.getLength() > 0) { this.removedRange.end = this.addedRange.end; this.changedAt = System.nanoTime(); }
    }

    public long remove(Column column, Object value) { // Remove old records with smallest values - less than the specified threshold (think of it as date of birth or id or timestamp)
        long insertId = column.findSortedFromStart(value); // It can be in any range: deleted, existing, added
        long toRemove = insertId - this.removedRange.end; // Records which are still not marked as removed
        if(toRemove > 0) {
            toRemove = Math.min(toRemove, this.getLength());
            this.remove(toRemove);
        }
        else {
            toRemove = 0;
        }
        return toRemove;
    }

    // Initialize to default state (e.g., empty set) by also forgetting change history
    // It is important to propagate this operation to all dependents as reset (not simply emptying) because some of them (like accumulation) have to forget/reset history and ids/references might become invalid
    // TODO: This propagation can be done manually or we can introduce a special method or it a special reset flag can be introduced which is then inherited and executed by all dependents during evaluation.
    public void reset() {
        long initialId = 0;
        this.addedRange.end = initialId;
        this.addedRange.start = initialId;
        this.removedRange.end = initialId;
        this.removedRange.start = initialId;

        this.changedAt = System.nanoTime();
    }

    //
    // Operations with data stored in columns (convenience methods)
    //

    public void getValues(long id, Map<String,Object> record) {
        for (Map.Entry<String, Object> field : record.entrySet()) {
            String name = field.getKey();
            Column col = this.getColumn(name);
            Object value = col.getValue(id);
            field.setValue(value);
        }
    }
    public void getValues(long id, List<Column> columns, List<Object> values) {
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object value = col.getValue(id);
            values.add(value);
        }
    }

    public void setValues(long id, Map<String,Object> record) {
        for (Map.Entry<String, Object> field : record.entrySet()) {
            String name = field.getKey();
            Column col = this.getColumn(name);
            Object value = field.getValue();
            col.setValue(id, value);
        }
    }
    public void setValues(long id, List<Column> columns, List<Object> values) {
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object value = values.get(i);
            col.setValue(id, value);
        }
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

        // Add what evaluation of product condition requires (except for this table columns)
        if(this.whereLambda != null && this.whereParameterPaths != null) {
            List<Column> cols = ColumnPath.getColumns(this.whereParameterPaths);
            for(Column col : cols) {
                if(col.getInput() == this) continue; // This table columns will be evaluated during population and hence during product evaluation, so we exclude them
                deps.add(col);
            }
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
        // Really evaluate
        //

        // If there exists (incoming) project-columns then its is a project-table - skip full population
        boolean isProj = false;
        for(Column col : this.schema.getColumns()) {
            if(col.getOperationType() != OperationType.PROJECT) continue;
            isProj = true;
            break;
        }
        if(isProj) {
            // Only reset to initial state (empty). Population will be performed by project columns
            this.reset();
        }
        // Else populate it using its own operation
        else {
            this.operation.evaluate();
            this.errors.addAll(this.operation.getErrors());
        }

    }

    //
    // Table (operation) kind
    //

    Operation operation;

    @Override
    public OperationType getOperationType() {
        if(this.operation == null && this.whereLambda == null) return OperationType.NOOP;
        else if (this.operation == null && this.whereLambda != null) return OperationType.PRODUCT;
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
        this.errors.clear();

        this.whereLambda = null;
        this.whereParameterPaths = new ArrayList<>();

        this.operation = null;
        this.definitionChangedAt = System.nanoTime();

        // TODO: Should we retains its current population (and if not, then has it to be overwritten automatically later during population or not) or it has to be emptied manually if necessary

        // TODO: Error check: some other definitions might become invalid if they depend on this table
    }

    //
    // Product table
    //

    EvalCalculate whereLambda;
    List<ColumnPath> whereParameterPaths = new ArrayList<>();

    public void product() {
        this.noop();

        this.operation = new OpProduct(this); // Create operation

        this.whereLambda = null;
        this.whereParameterPaths = null;

        if(this.hasDependency(this)) {
            this.noop();
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void product(EvalCalculate lambda, ColumnPath... paths) {
        this.noop();

        this.operation = new OpProduct(this); // Create operation

        this.whereLambda = lambda;
        this.whereParameterPaths = Arrays.asList(paths);

        if(this.hasDependency(this)) {
            this.noop();
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    public void product(EvalCalculate lambda, Column... columns) {
        this.noop(); // Reset operation

        this.operation = new OpProduct(this); // Create operation

        this.whereLambda = lambda;
        for(int i=0; i<columns.length; i++) {
            this.whereParameterPaths.add(new ColumnPath(columns[i]));
        }

        if(this.hasDependency(this)) {
            this.noop();
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    // Check whether the specified record (which is not in the table yet) satisfies the product condition
    // The record provides output values for the specified columns of this table
    protected boolean isWhereTrue(List<Object> record, List<Column> columns) {
        if(this.whereLambda == null || this.whereParameterPaths == null) return true;

        List<ColumnPath> paramPaths =  this.whereParameterPaths;
        Object[] paramValues = new Object[paramPaths.size() + 1];

        //
        // OPTIMIZE: This array has to be filled only once if we want to evaluate many records
        //
        int[] paramColumnIndex = new int[paramPaths.size()]; // For each param path, store the index in the record
        for(int i=0; i < paramColumnIndex.length; i++) {
            Column firstSegment = paramPaths.get(i).columns.get(0); // First segment
            int colIdx = columns.indexOf(firstSegment); // Index of the first segment in the record
            paramColumnIndex[i] = colIdx;
        }

        //
        // Prepare parameters
        //
        for(int p=0; p < paramPaths.size(); p++) {
            int keyNo = paramColumnIndex[p];
            Object recordValue = record.get(keyNo);
            paramValues[p] = paramPaths.get(p).getValueSkipFirst(recordValue);
        }

        //
        // Evaluate adder
        //
        boolean result;
        try {
            result = (boolean) this.whereLambda.evaluate(paramValues);
        }
        catch(BistroException e) {
            this.errors.add(e);
            return false;
        }
        catch(Exception e) {
            this.errors.add( new BistroException(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
            return false;
        }

        return result;
    }

    //
    // Range table
    //

    public void range(Object origin, Object period, Long length) {
        this.noop(); // Reset operation

        this.operation = new OpRange(this, origin, period, length); // Create operation

        if(this.hasDependency(this)) {
            this.noop();
            throw new BistroException(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly.");
        }
    }

    //
    // Search
    //

    // Important: Values must have the same type as the column data type - otherwise the comparision will not work
    // ISSUE: If not found, should output be NULL or -1? On one hand, we say that links are Long. But Long can be NULL. In future, it could be long which cannot be NULL.
    public long find(List<Object> values, List<Column> columns, boolean append) {

        //List<String> names = record.entrySet()..getNames();
        //List<Object> valuePaths = names.stream().map(x -> record.get(x)).collect(Collectors.<Object>toList());
        //List<Column> keyColumns = names.stream().map(x -> this.getSchema().getColumn(this.getName(), x)).collect(Collectors.<Column>toList());

        Range searchRange = this.getIdRange();
        long index = -1;
        for(long i=searchRange.start; i<searchRange.end; i++) { // Scan all records and compare
            // OPTIMIZATION: We could create or use an index and then binary search

            boolean found = true;
            for(int j=0; j<columns.size(); j++) {
                Object recordValue = values.get(j);
                Object columnValue = columns.get(j).getValue(i);

                // PROBLEM: The same number in Double and Integer will not be equal.
                //   SOLUTION 1: cast to some common type before comparison. It can be done in-line here or we can use utility methods.
                //   *SOLUTION 2: assume that the valuePaths have the type of the column, that is, the same comparable numeric type
                //   SOLUTION 3: always cast the value to the type of this column (either here or in the expression)

                // PROBLEM: Object.equals does not handle null's correctly
                //   *SOLUTION: Use .Objects.equals (Java 1.7), ObjectUtils.equals (Apache), or Objects.equal (Google common), a==b (Kotlin, translated to "a?.equals(b) ?: (b === null)"

                // Nullable comparison. If both are not null then for correct numeric comparison they must have the same type
                if( !Objects.equals(recordValue, columnValue) ) {
                    found = false;
                    break;
                }
            }

            if(found) {
                index = i;
                break;
            }
        }

        // If not found then add if requested
        if(index < 0 && append) {

            // Check if this record satisfies the product condition and we really can add it
            boolean whereTrue = this.isWhereTrue(values, columns);
            if(whereTrue) {
                // Really add
                index = this.add();
                this.setValues(index, columns, values);
            }
        }

        return index;
    }

    protected List<Column> getKeyColumns() { // Get all columns the domains of which have to be combined (non-primitive key-columns)
        List<Column> ret = new ArrayList<>();
        for(Column col : this.getColumns()) {
            if(!col.isKey()) continue; // Skip non-key columns
            ret.add(col);
        }
        return ret;
    }

    protected List<Column> getProjColumns() { // Get all incoming project-columns
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

        this.reset();

        this.changedAt = 0; // Very old - need to be evaluated
    }
}
