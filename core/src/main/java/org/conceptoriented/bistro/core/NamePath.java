package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Ab arbitrary sequence of names which normally represents a column path possibly with a table name as the first elemetn.
 */
public class NamePath {

	public List<String> names = new ArrayList<String>();

	// Assume that it is a column path, resolve all individual keyColumns relative to the specified table
	// The returned array contains as many elements as the names in the sequence
	public ColumnPath resolveColumns(Table table) {
		List<Column> result = new ArrayList<Column>();

		Schema schema = table.getSchema();

		for(String name : names) {
			Column column = schema.getColumn(table.getName(), name);
			if(column == null) { // Cannot resolve
				break;
			}
			result.add(column);
			table = column.getOutput();
		}

		return new ColumnPath(result);
	}

	// Assume that the sequence is a fully qualified name of a column
	public Column resolveColumn(Schema schema, Table table) { // Table is used only if no table name is available

		String tableName = this.getTableName();
		if(tableName == null) {
			if(table != null)
				tableName = table.getName();
			else
				return null;
		}

		Column column = null;
		if(getColumnName() != null) {
			column = schema.getColumn(tableName, getColumnName());
		}

		return column;
	}

	public String getTableName() {
		if(names.size() < 2) return null;
		return names.get(names.size()-2);
	}

	public String getColumnName() {
		if(names.size() < 1) return null;
		return names.get(names.size()-1); // Last segment
	}

	@Override
	public String toString() {
		StringBuffer ret = new StringBuffer();
		for(String n : this.names) {
			ret.append("["+n+"].");
		}
		if(ret.length() > 1) {
			return ret.substring(0, ret.length()-1);
		}
		else  {
			return ret.toString();
		}
	}

	public NamePath(List<String> names) {
		this.names.addAll(names);
	}

	public NamePath(String name) {
		this.names.add(name);
	}

	public NamePath() {
	}
}
