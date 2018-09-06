package org.conceptoriented.bistro.core.data;

import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.core.TableData;
import org.conceptoriented.bistro.core.Range;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class TableDataImpl implements TableData {

    private final UUID id;
    public UUID getId() {
        return this.id;
    }

    private Table table;

    @Override
    public long add() { // Add new elements with the next largest id. The created id is returned
        this.table.getColumns().forEach( x -> x.getData().add() );
        this.addedRange.end++; // Grows along with valid ids
        this.changedAt = System.nanoTime();
        return this.addedRange.end - 1; // Return id of the added element
    }

    @Override
    public Range add(long count) {
        this.table.getColumns().forEach( x -> x.getData().add(count) );
        this.addedRange.end += count; // Grows along with valid ids
        this.changedAt = System.nanoTime();
        return new Range(this.addedRange.end - count, this.addedRange.end); // Return ids of added elements
    }

    @Override
    public long remove() { // Remove oldest elements with smallest ids. The removed id is returned.
        this.table.getColumns().forEach( x -> x.getData().remove() );
        if(this.getLength() > 0) { this.removedRange.end++; this.changedAt = System.nanoTime(); }
        return this.removedRange.end - 1; // Id of the removed record (this id is not valid anymore)
    }

    @Override
    public Range remove(long count) {
        long toRemove = Math.min(count, this.getLength());
        if(toRemove > 0) { this.removedRange.end += toRemove; this.changedAt = System.nanoTime(); }
        return new Range(this.removedRange.end - toRemove, this.removedRange.end);
    }

    @Override
    public void removeAll() {
        if(this.getLength() > 0) { this.removedRange.end = this.addedRange.end; this.changedAt = System.nanoTime(); }
    }

    @Override
    public long remove(Column column, Object value) { // Remove old records with smallest values - less than the specified threshold (think of it as date of birth or id or timestamp)
        long insertId = column.getData().findSortedFromStart(value); // It can be in any range: deleted, existing, added
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

    @Override
    public void reset() {
        long initialId = 0;
        this.addedRange.end = initialId;
        this.addedRange.start = initialId;
        this.removedRange.end = initialId;
        this.removedRange.start = initialId;

        this.changedAt = System.nanoTime();
    }

    @Override
    public void getValues(long id, Map<String,Object> record) {
        for (Map.Entry<String, Object> field : record.entrySet()) {
            String name = field.getKey();
            Column col = this.table.getColumn(name);
            Object value = col.getData().getValue(id);
            field.setValue(value);
        }
    }
    @Override
    public void getValues(long id, List<Column> columns, List<Object> values) {
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object value = col.getData().getValue(id);
            values.add(value);
        }
    }
    @Override
    public void setValues(long id, Map<String,Object> record) {
        for (Map.Entry<String, Object> field : record.entrySet()) {
            String name = field.getKey();
            Column col = this.table.getColumn(name);
            Object value = field.getValue();
            col.getData().setValue(id, value);
        }
    }
    @Override
    public void setValues(long id, List<Column> columns, List<Object> values) {
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object value = values.get(i);
            col.getData().setValue(id, value);
        }
    }

    @Override
    public long findValues(List<Object> values, List<Column> columns) {

        Range searchRange = this.getIdRange();
        long index = -1;
        for(long i=searchRange.start; i<searchRange.end; i++) { // Scan all records and compare
            // OPTIMIZATION: We could create or use an index and then binary search

            boolean found = true;
            for(int j=0; j<columns.size(); j++) {
                Object recordValue = values.get(j);
                Object columnValue = columns.get(j).getData().getValue(i);

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

        return index; // Negative if not found
    }

    //
    // Tracking changes.
    // Change status is delta between previous state and current state (as a number of added and removed records)
    //

    protected Range addedRange = new Range(); // Newly added ids
    @Override
    public Range getAddedRange() {
        return this.addedRange;
    }

    protected Range removedRange = new Range(); // Removed ids
    @Override
    public Range getRemovedRange() {
        return this.removedRange;
    }

    @Override
    public Range getIdRange() {
        return new Range(this.removedRange.end, this.addedRange.end); // Derived from added and removed
    }

    @Override
    public long getLength() {
        return this.addedRange.end - this.removedRange.end; // All non-removed records
    }

    //
    // Tracking changes (delta)
    //

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

    //
    // Creation
    //

    public TableDataImpl(Table table) {
        this.id = UUID.randomUUID();

        this.table = table;

        this.reset();
        this.changedAt = 0; // Very old - need to be evaluated
    }
}
