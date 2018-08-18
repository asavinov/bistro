package org.conceptoriented.bistro.core.data;

import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.core.TableData;
import org.conceptoriented.bistro.core.Range;

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

    // Initialize to default state (e.g., empty set) by also forgetting change history
    // It is important to propagate this operation to all dependents as reset (not simply emptying) because some of them (like accumulation) have to forget/reset history and ids/references might become invalid
    // TODO: This propagation can be done manually or we can introduce a special method or it a special reset flag can be introduced which is then inherited and executed by all dependents during evaluation.
    @Override
    public void reset() {
        long initialId = 0;
        this.addedRange.end = initialId;
        this.addedRange.start = initialId;
        this.removedRange.end = initialId;
        this.removedRange.start = initialId;

        this.changedAt = System.nanoTime();
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
