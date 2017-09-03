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

	private final Column column;
	public Column getColumn() {
		return this.column;
	}

	private final UUID id;
	public UUID getId() {
		return this.id;
	}


	//
	// Data
	//
	
	// Array with output values. Very first element corresponds to the oldest existing id. Very last element corresponds to the newest existing id.
	// Note that some (oldest, in the beginning) elements can be marked for deletion, that is, it is garbage and they are stored with the only purpose to do evaluation by updating the state of other elements 
	private Object[] values;
	
	// Id of the very first element in the array with 0th offset
	private long startId = 0;
	public int id2offset(long id) {
		return (int) (id - this.startId);
	}

	// It is physical size of all values including deleted, clean and new. It must be equal to the table size
	public long getLength() {
		return this.delRange.getLength() + this.cleanRange.getLength() + this.newRange.getLength();
	}
	

	//
	// Data access
	//
	
	public Object getValue(long id) {
		return this.values[id2offset(id)];
	}
	public void setValue(long id, Object value) {
		this.values[id2offset(id)] = value;
		this.isChanged = true; // Mark column as dirty
	}
	// Convenience method. The first element in the path must be this column. 
	public Object getValue(List<Column> columns, long id) {
		Object out = id;
		for(Column col : columns) {
			out = col.getData().getValue((long)out);
			if(out == null) break;
		}
		return out;
	}

	public long appendValue(Object value) {
		// Cast the value to type of this column
		if(this.column.getOutput().getName().equalsIgnoreCase("String")) {
			try { value = value.toString(); } 
			catch (Exception e) { value = ""; }
		}
		else if(this.column.getOutput().getName().equalsIgnoreCase("Double") || this.column.getOutput().getName().equalsIgnoreCase("Integer")) {
			if(value instanceof String) {
				try { value = NumberFormat.getInstance(Locale.US).parse(((String)value).trim()); } 
				catch (ParseException e) { value = Double.NaN; }
			}
		}
		
		//
		// Really append (after the last row) and mark as new
		//
		this.values[id2offset(this.newRange.end)] = value;
		this.newRange.end++;

		return this.newRange.end-1;
	}

	// They can be deleted either physically immediately or marked for deletion for future physical deletion (after evalution or gargabge collection)
	// We delete only oldest records with lowest ids
	public void remove(long count) { // Remove the oldest records with lowest ids
		// TODO:
	}
	public void remove(Range range) { // Delete the specified range of input ids starting from clean range and then continue with new range by increasing the available del range (they must be the oldest records with lowest ids)
		this.delRange = range.uniteWith(this.delRange);
		this.cleanRange = this.delRange.delFromStartOf(this.cleanRange);
		this.newRange = this.delRange.delFromStartOf(this.newRange);

		// Optimize
		this.gc(); 
	}
	public void remove() { // Delete all input ids
		remove(this.getIdRange());
	}

	public void gc() { // Physically remove del range
		// Remove del range (it might require evaluation)
		delRange.start = delRange.end;

		// Determine (free, unnecessary) interval to moved
		long offset = delRange.start - this.startId;

		// New offset
		this.startId += offset;

		// Move cells backward
		System.arraycopy(this.values, (int)offset, this.values, 0, (int)this.getLength());
	}

	//
	// Data dirty state: added/removed inputs (set population), and change outputs (function updated)
	//
	
	// Output value changes (change, set, reset function output).

	// If some output values has been changed (manually) and hence evaluation of dependent columns might be needed.
	// Note that this flag does not change the dirty status of this column - it changes the dirty status of all columns that depend on it
	public boolean isChanged = false;
	// This flag can be set either directly from outside or by evaluation procedure from inside.
	// So we need to understand how to use it for dependencies



	// Input range changes (additions and deletions of the input set).
	// 5,6,7,...,100,...,1000,1001,...
	// [del)[clean)[new)
	// [rowRange) - all records that physically exist and can be accessed including new, up-to-date and deleted

	// Deleted but not evaluated (garbage): Some input elements have been marked for deletion but not deleted yet because the deletion operation needs to be evaluated before physical deletion
	// Records to be deleted after the next re-evaluation
	// Immediately after evaluation these records have to be physically deleted (otherwise the data state will be wrong)
	// Deleted records are supposed to have lowest ids (by assuming that we delete only old records)
	protected Range delRange = new Range();
	public Range getDelRange() {
		return new Range(delRange);
	}

	// Clean (evaluated): Input elements which store up-to-date outputs and hence need not to be evaluated
	// These records have been already evaluated (clean)
	// We need to store start and end rows
	// Alternatively, we can compute this range as full range minus added and deleted
	protected Range cleanRange = new Range();
	public Range getCleanRange() {
		return new Range(cleanRange);
	}

	// Added but not evaluated: Some input elements have been physically added but their output not evaluated (if formula defined) or not set (if output is set manually)
	// Records added but not evaluated yet (dirty). They are supposed to be evaluated in the next iteration.
	// Immediately after evaluation they need to be marked as clean
	// New records have highest ids by assuming that they are newest records
	protected Range newRange = new Range();
	public Range getNewRange() {
		return new Range(newRange);
	}
	


	
	// All currently existing (non-deleted) elements
	public Range getIdRange() {
		return new Range(getCleanRange().start, getNewRange().end);
	}



	// Mark clean records as dirty (new). Deleted range does not change (we cannot undelete them currently). It is a manual way to trigger re-evaluation.
	protected void markCleanAsNew() {
		// [del)[clean)[new)
		cleanRange.end = cleanRange.start; // No clean records
		newRange.start = cleanRange.start; // All new range
	}

	// Mark dirty records as clean. It is supposed to be done by evaluation procedure.
	protected void markNewAsClean() {
		// [del)[clean)[new)
		cleanRange.end = newRange.end; // All clean records
		newRange.start = newRange.end; // No new range
	}

	// Mark all records as deleted. Note that it is only marking - for real deletion use other methods.
	protected void markAllAsDel() {
		// [del)[clean)[new)

		delRange.end = newRange.end;
		
		cleanRange.start = newRange.end;
		cleanRange.end = newRange.end;
		
		newRange.start = newRange.end;
		newRange.end = newRange.end;
	}



	public ColumnData(Column column) {
		this.column = column;
		this.id = UUID.randomUUID();

		// Initialize storage
		this.values = new Object[1000];

		// Initialize ranges according to the input table (all records new)
		this.newRange = new Range(this.column.getInput().getIdRange());
		this.delRange = new Range(this.newRange.start, this.newRange.start);
		this.cleanRange = new Range(this.newRange.start, this.newRange.start);
	}
}
