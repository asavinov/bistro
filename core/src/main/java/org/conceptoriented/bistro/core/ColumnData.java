package org.conceptoriented.bistro.core;

import java.util.Arrays;
import java.util.UUID;

/**
 * It is responsible for explicit representation of a function, that is, a mapping from input ids to output columnPaths.
 * This representation can be changed by setting outputs for certain inputs. And it is possible to request outputs.
 */
public class ColumnData {

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

    private Object defaultValue = null;
    public Object getDefaultValue() { return this.defaultValue; }
    public void setDefaultValue(Object value) { this.defaultValue = value; }

    private int startIdOffset = 0; // Cell of the array where the start id is stored

	private int id2offset(long id) {
		return this.startIdOffset + ((int) (id - this.idRange.start));
	}

	//
	// Data access
	//
	
	protected Object getValue(long id) { return this.values[id2offset(id)]; }

	protected void setValue(long id, Object value) { this.values[id2offset(id)] = value; }

	protected void setValue(Object value) { // All
		Arrays.fill(this.values, startIdOffset, (int)(startIdOffset+this.idRange.getLength()), value);
	}
	protected void setValue() { // All default value (depends on the column data type)
		this.setValue(defaultValue);
	}

	protected void add() { this.add(1); }
    protected void add(long count) { // Remove the oldest records with lowest ids

		// Check if not enough space and allocate more if necessary
        int additionalSize = (this.startIdOffset + (int)idRange.getLength() + (int)count) - this.values.length;
        if(additionalSize > 0) { // More space is needed
            additionalSize = INCREMENT_SIZE + additionalSize / INCREMENT_SIZE; // How many increments we need to cover the additional size
            this.values = Arrays.copyOf(this.values, this.values.length + additionalSize);
        }

        // Initialize
        Arrays.fill(this.values, this.id2offset(this.idRange.end), this.id2offset(this.idRange.end) + (int)count, this.defaultValue);

        this.idRange.end += count;
    }

    protected void remove() { this.remove(1); }
	protected void remove(long count) { // Remove the oldest records with lowest ids

        // Delete
        this.idRange.start += count;
        this.startIdOffset += count;

        // Check if there is enough space to free (garbage collection)
        if(this.startIdOffset > INCREMENT_SIZE) {
            // Shift columnPaths to the beginning
            System.arraycopy(this.values, this.startIdOffset, this.values, 0, (int)this.idRange.getLength());
            this.startIdOffset = 0;

            if(this.values.length > INITIAL_SIZE + INITIAL_SIZE) { // Do not make smaller than initial size
                int additionalSize = this.values.length - (int) idRange.getLength(); // Unused space
                additionalSize = (additionalSize / INCREMENT_SIZE) * INCREMENT_SIZE; // How much (whole increments) we want to remove
                this.values = Arrays.copyOf(this.values, this.values.length - additionalSize);
            }
        }
	}

	public ColumnData(long start, long end) {
		this.id = UUID.randomUUID();

		// Initialize storage
		this.values = new Object[INITIAL_SIZE];

		// Initialize ranges according to the input table (all records new)
		this.idRange.start = start;
        this.idRange.end = end;
	}
}
