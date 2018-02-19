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

    public boolean isPrimitive() {
        if(name.equalsIgnoreCase("Object") || name.equalsIgnoreCase("Double") || name.equalsIgnoreCase("Integer") || name.equalsIgnoreCase("String")) {
            return true;
        }
        return false;
    }

    public List<Column> getColumns() {
        return this.schema.getColumns(this);
    }
    public Column getColumn(String name) {
        return this.schema.getColumn(this, name);
    }

    //
    // Table as a set of ids. Iteration through this collection (table readers and writers).
    //

    protected Range idRange = new Range(); // Full id range of records from start to end
    public Range getIdRange() {
        return this.idRange;
    }

    public long getLength() {
        return this.idRange.getLength();
    }

    //
    // Operations with ids (table population)
    //

    public long add() { // Add new elements with the next largest id. The created id is returned
        this.getColumns().forEach( x -> x.add() );
        this.idRange.end++;
        return this.idRange.end - 1;
    }

    public Range add(long count) {
        this.getColumns().forEach( x -> x.add(count) );
        this.idRange.end += count;
        return new Range(this.idRange.end - count, this.idRange.end);
    }

    public long remove() { // Remove oldest elements with smallest ids. The deleted id is returned.
        this.getColumns().forEach( x -> x.remove() );
        this.idRange.start++;
        return this.idRange.start - 1;
    }

    public Range remove(long count) {
        this.getColumns().forEach( x -> x.remove(count) );
        if(count > 0) { // Remove oldest
            this.idRange.start += count;
            return new Range(this.idRange.start - count, this.idRange.start);
        }
        else if(count < 0) { // Remove newest
            this.idRange.end += count; // End is decreased because count is negative
            return new Range(this.idRange.end, this.idRange.end - count);
        }
        else {
            return new Range(this.idRange.start, this.idRange.start);
        }
    }

    protected void removeAll() {
        this.getColumns().forEach( x -> x.removeAll() );
        this.idRange.start = 0;
        this.idRange.end = 0;
    }

    //
    // Operations with multiple column valuePaths
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

        if(this.getDefinitionType() == TableDefinitionType.NOOP || this.definition == null) {
            return deps;
        }

        deps = this.definition.getDependencies();
        if(deps == null) deps = new ArrayList<>();

        // Add what evaluation of where condition requires (except for this table columns)
        if(this.expressionWhere != null) {
            List<ColumnPath> paths = this.expressionWhere.getParameterPaths();
            List<Column> cols = ColumnPath.getColumns(paths);
            for(Column col : cols) {
                if(col.getInput() == this) continue; // This table columns will be evaluated during population and hence during where evaluation, so we exclude them
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
    public List<Element> getDependants() {
        return new ArrayList<>();
    }
    @Override
    public boolean hasDependant(Element element) {
        for(Element dep : this.getDependants()) {
            if(dep == this) return true;
            if(dep.hasDependant(element)) return true;// Recursion
        }
        return false;
    }

    private List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getDefinitionErrors() { // Empty list in the case of no errors
        List<BistroError> ret = new ArrayList<>();
        ret.addAll(this.definitionErrors);

        if(this.definition != null) {
            ret.addAll(this.definition.getErrors());
        }

        return ret;
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
        if(definitionType == TableDefinitionType.PROD) {
            this.getProjColumns().forEach(x -> x.setDirty()); // All proj-columns have to be marked dirty too because they will populate this table
        }
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
        this.populate();
    }

    //
    // Populate
    //

    public void populate() {

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
        if(this.definition != null) {

            // If there exist proj-columns then its is a proj-table - skip full population
            boolean isProj = false;
            for(Column col : this.schema.getColumns()) {
                if(col.getDefinitionType() != ColumnDefinitionType.PROJ) continue;
                isProj = true;
                break;
            }
            if(!isProj) {
                this.definition.populate();
                this.executionErrors.addAll(this.definition.getErrors());
            }
        }

        if(this.executionErrors.size() == 0) {
            this.isDirty = false; // Clean the state (remove dirty flag)
        }
        else {
            this.isDirty = true; // Evaluation failed
        }
    }

    //
    // Table (definition) kind
    //

    TableDefinition definition; // It is instantiated by prod-prod methods (or definition errors are added)

    protected TableDefinitionType definitionType;
    public TableDefinitionType getDefinitionType() {
        return this.definitionType;
    }
    public void setDefinitionType(TableDefinitionType definitionType) {
        this.definitionType = definitionType;
        this.definitionErrors.clear();
        this.executionErrors.clear();

        if(this.definitionType == TableDefinitionType.NOOP) {
            this.definition = null;
            this.expressionWhere = null;
        }

        this.setDirty();

        this.removeAll();
    }
    public boolean isDerived() {
        if(this.definitionType == TableDefinitionType.NOOP) {
            return false;
        }
        return true;
    }

    //
    // Noop table
    //

    // Note that the table retains its current population (which will not be overwritten automatically later during population) and it has to be emptied manually if necessary
    public void noop() {
        this.setDefinitionType(TableDefinitionType.NOOP);
    }

    //
    // Product table
    //

    public void prod() {
        this.setDefinitionType(TableDefinitionType.PROD); // Reset definition

        this.definition = new TableDefinitionProd(this); // Create definition

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    //
    // Where
    //

    Expression expressionWhere;

    public void where(Evaluator lambda, ColumnPath... paths) {
        this.setDefinitionType(TableDefinitionType.PROD);

        this.expressionWhere = new Expr(lambda, paths);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    public void where(Evaluator lambda, Column... columns) {
        this.setDefinitionType(TableDefinitionType.PROD);

        this.expressionWhere = new Expr(lambda, columns);

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
            return;
        }
    }

    public void where(Expression expr) {
        this.setDefinitionType(TableDefinitionType.PROD);

        this.expressionWhere = expr;

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
        }
    }

    // Check whether the specified record (which is not in the table yet) satisfies the where condition
    // The record provides output values for the specified columns of this table
    protected boolean isWhereTrue(List<Object> record, List<Column> columns) {
        if(this.expressionWhere == null) return true;

        List<ColumnPath> paramPaths =  this.expressionWhere.getParameterPaths();
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
        // Prepare expression parameters
        //
        for(int p=0; p < paramPaths.size(); p++) {
            int keyNo = paramColumnIndex[p];
            Object recordValue = record.get(keyNo);
            paramValues[p] = paramPaths.get(p).getValueSkipFirst(recordValue);
        }

        //
        // Evaluate expression
        //
        boolean result;
        try {
            result = (boolean) this.expressionWhere.eval(paramValues);
        }
        catch(BistroError e) {
            this.executionErrors.add(e);
            return false;
        }
        catch(Exception e) {
            this.executionErrors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
            return false;
        }

        return result;
    }

    //
    // Range table
    //

    public void range(Object origin, Object period, Long length) {
        this.setDefinitionType(TableDefinitionType.RANGE); // Reset definition

        this.definition = new TableDefinitionRange(this, origin, period, length); // Create definition

        if(this.hasDependency(this)) {
            this.definitionErrors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Cyclic dependency.", "This column depends on itself directly or indirectly."));
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
            index = this.add();
            this.setValues(index, columns, values);
        }

        return index;
    }

    public long findNumber(Number value, boolean append) { // Find in range table (using inequality)
        if(this.getDefinitionType() != TableDefinitionType.RANGE) {
            return -1;
        }

        TableDefinitionRange def = (TableDefinitionRange)this.definition;

        Column rangeColumn = def.getRangeColumn();
        Column intervalColumn = def.getIntervalColumn();

        Range searchRange = this.getIdRange();

        // Data in a range table is known to be sorted
        long index = rangeColumn.findSorted(value);

        if(index >= 0) { // Found. On the interval border
            ;
        }
        else if(index < 0) { // Not found. Inside interval.
            index = -index - 1; // Insertion index

            if(index == searchRange.start) { // Before first element
                index = -1;
            }
            if(index >= searchRange.end) { // After last element
                Number lastValue = (Number)rangeColumn.getValue(searchRange.end-1);
                if((double)value >= (double)lastValue + (double)def.period) { // Too high value
                    index = -1;
                }
            }
            else { // Between two raster points
                Number rangeValue = (Number)rangeColumn.getValue(index);
                index = index - 1; // Left border
            }
        }

        // If not found then add if requested
        if(index < 0 && append) {
            ; // TODO: Not implemented
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

    protected List<Column> getProjColumns() { // Get all incoming proj-columns
        List<Column> ret = new ArrayList<>();
        for(Column col : this.getSchema().getColumns()) {
            if(col.getOutput() != this) continue;
            if(col.getDefinitionType() != ColumnDefinitionType.PROJ) continue; // Skip non-key columns
            ret.add(col);
        }
        return ret;
    }

    //
    // Serialization and construction
    //

    @Override
    public String toString() {
        return "[" + name + "]";
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

        // Where its instances come from
        this.definitionType = TableDefinitionType.NOOP;
    }
}
