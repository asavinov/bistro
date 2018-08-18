package org.conceptoriented.bistro.core;

import java.util.Arrays;
import java.util.UUID;

/**
 * It is responsible for explicit representation of a function, that is, a mapping from input ids to output columnPaths.
 * This representation can be changed by setting outputs for certain inputs. And it is possible to request outputs.
 */
public class ColumnData implements ColumnDataInterface {

    private final UUID id;
    public UUID getId() {
        return this.id;
    }

    //
    // Data
    //

    private Range idRange = new Range(); // All valid input ids for which outputs are stored - other ids are not valid and will produce exception or arbitrary value (garbage)

    private static int INITIAL_SIZE = 10;
    private static int INCREMENT_SIZE = 5;
    private Object[] values; // This array stores the output columnPaths

    private int startIdOffset = 0; // Cell of the array product the start id is stored

    private int id2offset(long id) {
        return this.startIdOffset + ((int) (id - this.idRange.start));
    }
    private long offset2id(int offset) {
        return this.idRange.start + (offset - this.startIdOffset);
    }

    //
    // Output values
    //

    @Override
    public Object getValue(long id) { return this.values[id2offset(id)]; }

    // Note: we do not set the change flag by assuming that only newly added records are changed - if it is not so then it has to be set manually
    // Note: methods are not safe - they do not check the validity of arguments (ids, values etc.)

    @Override
    public void setValue(long id, Object value) { this.values[id2offset(id)] = value; }

    @Override
    public void setValue(Range range, Object value) {
        Arrays.fill(
                this.values,
                this.id2offset(range.start),
                this.id2offset(range.end),
                value
        );
    }
    @Override
    public void setValue(Range range) { // Default value
        this.setValue(range, this.defaultValue);
    }

    @Override
    public void setValue(Object value) {
        Arrays.fill(
                this.values,
                this.startIdOffset,
                (int)(this.startIdOffset + this.idRange.getLength()),
                value
        );
        this.setChanged();
    }
    @Override
    public void setValue() { // Default value
        this.setValue(this.defaultValue);
        this.setChanged();
    }

    private Object defaultValue = null;
    @Override
    public Object getDefaultValue() { return this.defaultValue; }
    @Override
    public void setDefaultValue(Object value) {
        this.defaultValue = value;
        this.setChanged();
    }

    //
    // Input range
    //

    @Override
    public void add() {
        this.add(1);
        this.isChanged = true;
    }
    @Override
    public void add(long count) { // Remove the oldest records with lowest ids

        // Check if not enough space and allocate more if necessary
        int additionalSize = (this.startIdOffset + (int)idRange.getLength() + (int)count) - this.values.length;
        if(additionalSize > 0) { // More space is needed
            additionalSize = ((additionalSize/INCREMENT_SIZE) + 1) * INCREMENT_SIZE; // How many increments we need to cover the additional size
            this.values = Arrays.copyOf(this.values, this.values.length + additionalSize);
        }

        // Initialize
        Arrays.fill(
                this.values,
                this.id2offset(this.idRange.end),
                this.id2offset(this.idRange.end) + (int)count,
                this.defaultValue
        );

        this.idRange.end += count;

        this.isChanged = true;
    }

    @Override
    public void remove() {
        this.remove(1);
        this.isChanged = true;
    }
    @Override
    public void remove(long count) { // Remove the specified number of oldest records
        this.startIdOffset += count;
        this.idRange.start += count;

        this.isChanged = true;

        this.gc();
    }
    @Override
    public void removeAll() {
        this.remove(this.idRange.getLength());
        this.isChanged = true;
    }

    @Override
    public void reset(long start, long end) {
        // Allocate memory
        this.values = new Object[INITIAL_SIZE];

        // Initially no data but the ids start from what is specified in parameters
        this.idRange.start = start;
        this.idRange.end = start;

        // Now the end will move and space will be added if necessary
        this.add(end - start);

        this.setValue(); // Set default values

        this.isChanged = true;
    }

    @Override
    public void gc() { // Garbage collection. Free some space if there is enough in the beginning of the array
        if(this.startIdOffset > INCREMENT_SIZE) {
            // Shift values to the beginning
            System.arraycopy(
                    this.values,
                    this.startIdOffset,
                    this.values,
                    0,
                    (int)this.idRange.getLength()
            );
            this.startIdOffset = 0;

            // Free space at the end of the allocated array
            if(this.values.length >= INITIAL_SIZE + INCREMENT_SIZE) { // Do not make smaller than initial size
                int additionalSize = this.values.length - (int) idRange.getLength(); // Unused space
                additionalSize = (additionalSize / INCREMENT_SIZE) * INCREMENT_SIZE; // How much (whole increments) we want to remove
                this.values = Arrays.copyOf(this.values, this.values.length - additionalSize);
            }
        }
    }

    // Return insert index
    @Override
    public long findSorted(Object value) {

        // The data is supposed to be sorted (for example, range table or time stamps)
        int insertIndex = Arrays.binarySearch(
                this.values,
                this.startIdOffset,
                (int)(this.startIdOffset + this.idRange.getLength()),
                value
        );
        // TODO: In the case of multiple equal values, the index can be any. It is better if we return the first or last element of such intervals. So we need to check for equality and return start or end.

        long id = offset2id(insertIndex);

        return id;
    }

    @Override
    public long findSortedFromStart(Object value) { // Find insertion index with the value strictly less than the specified value

        // Values must be comparable (implement Comparable interface)

        // Start from the last/old/smallest ids and move in the loop until a greater or equal value is found
        int start = this.id2offset(this.idRange.start);
        int end = this.id2offset(this.idRange.end);
        for(int i = start; i < end; i++) {
            Object val = this.values[i];
            if(val == null) continue;
            if(((Comparable)val).compareTo(value) < 0) { // It is still small
                continue;
            }
            end = i;
            break;
        }

        long id = offset2id(end);

        return id;
    }

    //
    // Tracking changes (delta)
    //

    private long changedAt; // Time of latest change
    @Override
    public long getChangedAt() {
        return this.changedAt;
    }
    @Override
    public void setChangedAt(long changedAt) {
        this.changedAt = changedAt;
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

    //
    // Creation
    //

    public ColumnData(long start, long end) {
        this.id = UUID.randomUUID();

        this.reset(start, end);
    }
}
