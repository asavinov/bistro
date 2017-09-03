package org.conceptoriented.bistro.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class Table {
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
		if(name.equalsIgnoreCase("Double") || name.equalsIgnoreCase("Integer") || name.equalsIgnoreCase("String")) return true;
		return false;
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
	// Rules for automatic population and de-population (similar to auto-evaluation)
	//

	// Max age. Old records will be automatically deleted. 0 means immediate deletion of new records. MAX, NULL or -1 mean any age and hence no auto-deletion.  
	protected long maxAge = -1;  

	// If more rows are added then then the oldest will be marked for deletion.  
	// The real deletion happens only after evaluation.
	protected long maxLength = -1;  
	public long getMaxLength() {
		return this.maxLength;
	}
	public void setMaxLength(long maxLength) {
		
		if(maxLength == this.maxLength) {
			return;
		}
		if(maxLength < 0) {
			// Remove all del-markers. Any length is possible.
		}
		else if(maxLength > this.maxLength) {
			// Remove some del-markers until fit into new max length
		}
		else {
			// Add some del-markers to fit into the new max length
		}

		this.maxLength = maxLength;
	}
	
	// Decide if deletion is needed and delete
	public void autodelete() {
		if(this.maxLength == 0) {
			return;
		}

		// length <= maxLength
		long excess = this.idRange.getLength() - this.maxLength;
		if(excess <= 0) {
			return;
		}

		// There are more records than the maximum. Delete last records
		Range delRange = new Range(this.idRange.start, this.idRange.start + excess);
		this.remove(delRange);
	}

	//
	// Operations with records
	//

	Instant appendTime = Instant.now(); // Last time a record was (successfully) appended. It is equal to the time stamp of the last record.
	public void setAppendTime() {
		this.appendTime = Instant.now();
	}
	public Duration durationFromLastAppend() {
		return Duration.between(this.appendTime, Instant.now());
	}

	public long append(Record record) {

		// Get all outgoing columns
		List<Column> columns = this.schema.getColumns(this.getName());

		for(Column column : columns) { // We must append a new value to all columns even if it has not been provided (null)

			// Get value from the record
			Object value = record.get(column.getName());
			
			// Append the value to the column (even if it is null). This will also update the dirty status of data (range).
			column.getData().appendValue(value);
		}
		
		this.idRange.end++;

		setAppendTime(); // Store the time of append operation

		// If too many records then mark some of them (in the beginning) for deletion (mark dirty)
		//this.autodelete();
		
		return this.idRange.end - 1;
	}
	public void append(List<Record> records, Map<String, String> columnMapping) {
		for(Record record : records) {
			this.append(record);
		}
	}

	public long find(Record record, boolean append) {

		List<String> names = record.getNames();
		List<Object> values = names.stream().map(x -> record.get(x)).collect(Collectors.<Object>toList());
		List<Column> columns = names.stream().map(x -> this.getSchema().getColumn(this.getName(), x)).collect(Collectors.<Column>toList());
		
		Range searchRange = this.idRange;
		long index = -1;
		for(long i=searchRange.start; i<searchRange.end; i++) { // Scan all records and compare

			boolean found = true;
			for(int j=0; j<names.size(); j++) {
				// TODO: The same number in Double and Integer will not be equal. So we need cast to some common type at some level of the system or here.
				Object recordValue = values.get(j);
				Object columnValue = columns.get(j).getData().getValue(i);

				// Compare two values of different types
				if(recordValue instanceof Number && columnValue instanceof Number) {
					if( ((Number) recordValue).doubleValue() != ((Number) columnValue).doubleValue() ) { found = false; break; }
					// OLD: if(!recordValue.equals(columnValue)) { found = false; break; }
				}
				else {
					// Compare nullable objects
					if( !com.google.common.base.Objects.equal(recordValue, columnValue) ) { found = false; break; }
				}
			}
			
			if(found) {
				index = i;
				break;
			}
		}
		
		// If not found then append if requested
		if(index < 0 && append) {
			index = this.append(record);
		}

		return index;
	}

	// Currently we assume that the deleted range can be only in the beginning (oldest records with smallest ids) - otherwise it will not work. Also, we assume that the deleted range is smaller than the number of records.
	public void remove(long count) { // Remove the oldest records with lowest ids

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		for(Column column : columns) { // We must delete from all columns
			column.getData().remove(count);
		}

		// Update the id range of the table by assuming that the specified deleted range is in the beginning
		this.idRange.start += count;
	}
	public void remove(Range range) { // Remove records in the specified range of ids

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		for(Column column : columns) { // We must delete from all columns
			column.getData().remove(range);
		}

		// Update the id range of the table by assuming that the specified deleted range is in the beginning
		this.idRange.start = range.end;
	}
	public void remove() { // Remove all records (full range of ids)
		remove(idRange);
	}

	public List<Record> read(Range range) {
		if(range == null) {
			range = this.idRange;
		}

		// Get all outgoing columns
		List<Column> columns = schema.getColumns(this.getName());

		List<Record> records = new ArrayList<Record>();
		for(long row = range.start; row < range.end; row++) {
			
			Record record = new Record();
			record.set("_id", row);

			for(Column column : columns) {
				// Get value from the record
				Object value = column.getData().getValue(row);
				// Store the value in the record
				record.set(column.getName(), value);
			}
			
			records.add(record);
		}
		
		return records;
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
	}

}
