package org.conceptoriented.bistro.core;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * It is responsible for explicit representation of a function, that is, a mapping from input ids to output values.
 * This representation can be changed by setting outputs for certain inputs. And it is possible to request outputs.
 * 
 * Another purpose of this class is to represent the dirty state of the function, that is, whether it is up-to-date.
 * The first part of the status is whether some outputs have been changed (each time an output is change, the flag is raised).
 * The second part of the status whether some inputs have been added or removed without being appropriately processed (for each add or remove of the input, this status is changed).
 * For added and removed inputs, their range can be stored, that is, it is possible to retrieve the range of new (added) input id and the range of removed (deleted) input ids.
 * 
 * There are also functions for reseting the corresponding dirty statuses. 
 * Normally dirty status is reset after the corresponding recorded changes have been processed, that is, propagated through the schema to other functions which depend on them.
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

	private Object[] values; // This array stores the output values

	private int startIdOffset = 0; // Cell of the array where the start id is stored

	private int id2offset(long id) {
		return this.startIdOffset + ((int) (id - this.idRange.start));
	}

	//
	// Data access
	//
	
	protected Object getValue(long id) {
		return this.values[id2offset(id)];
	}

	protected void setValue(long id, Object value) {
		this.values[id2offset(id)] = value;
	}

    // TODO: Maybe return the newly added (valid) range
	protected void add() { this.add(1); }
    protected void add(long count) { // Remove the oldest records with lowest ids
        // TODO: Allocate more or shift back if necessary
        this.values[id2offset(this.idRange.end)] = null;
        this.idRange.end++;
    }

    // TODO: Maybe return the old deleted (invalid) range
    protected void remove() { this.remove(1); }
	protected void remove(long count) { // Remove the oldest records with lowest ids
		// TODO:
		// Move cells backward after deletion
		//System.arraycopy(this.values, (int)offset, this.values, 0, (int)this.getLength());
	}

	public ColumnData(long start, long end) {
		this.id = UUID.randomUUID();

		// Initialize storage
		this.values = new Object[1000];

		// Initialize ranges according to the input table (all records new)
		this.idRange.start = start;
        this.idRange.end = end;
	}
}
