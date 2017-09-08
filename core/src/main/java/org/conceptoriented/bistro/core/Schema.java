package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Stream schema stores the complete data state and is able to consistently update it. 
 */
public class Schema {
	
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
	
	//
	// Tables
	//
	
	private List<Table> tables = new ArrayList<>();
	public List<Table> getTables() {
		return this.tables;
	}
	public Table getTable(String table) {
        Table ret = this.tables.stream().filter(x -> x.getName().equalsIgnoreCase(table)).findAny().orElse(null);
        return ret;
	}
	public Table getTableById(String id) {
        Table ret = this.tables.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
	}

	public Table createTable(String name) {
		Table tab = this.getTable(name);
		if(tab != null) return tab; // Already exists

		tab = new Table(this, name);
		this.tables.add(tab);
		return tab;
	}
    public void deleteTable(Table tab) {
		// Remove input columns
		List<Column> inColumns = this.columns.stream().filter(x -> x.getInput().equals(tab)).collect(Collectors.<Column>toList());
		this.columns.removeAll(inColumns);
		
		// Remove output columns
		List<Column> outColumns = this.columns.stream().filter(x -> x.getOutput().equals(tab)).collect(Collectors.<Column>toList());
		this.columns.removeAll(outColumns);
		
		// Remove table itself
		this.tables.remove(tab);
	}

	//
	// Columns
	//

	private List<Column> columns = new ArrayList<>();
	public List<Column> getColumns() {
		return this.columns;
	}
	public List<Column> getColumns(Table table) {
		List<Column> res = this.columns.stream().filter(x -> x.getInput().equals(table)).collect(Collectors.<Column>toList());
		return res;
	}
	public List<Column> getColumns(String table) {
		List<Column> res = this.columns.stream().filter(x -> x.getInput().getName().equalsIgnoreCase(table)).collect(Collectors.<Column>toList());
		return res;
	}
	public Column getColumn(Table table, String column) {
		Column ret = this.columns.stream().filter(x -> x.getInput().equals(table) && x.getName().equalsIgnoreCase(column)).findAny().orElse(null);
		return ret;
	}
	public Column getColumn(String table, String column) {
        Column ret = this.columns.stream().filter(x -> x.getInput().getName().equalsIgnoreCase(table) && x.getName().equalsIgnoreCase(column)).findAny().orElse(null);
        return ret;
	}
	public Column getColumnById(String id) {
        Column ret = this.columns.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
	}

	public Column createColumn(String name, Table  input, Table output) {
		Column col = new Column(this, name, input, output);
		this.columns.add(col);
		return col;
	}
	public Column createColumn(String name, String input, String output) {
		Column col = new Column(this, name, this.getTable(input), this.getTable(output));
		this.columns.add(col);
		return col;
	}
	public void deleteColumn(Column col) {
		this.columns.remove(col);
	}

	//
	// Evaluate (re-compute dirty, selected or all function outputs)
	//
	
	/**
	 * Evaluate all columns of the schema. Only dirty columns without errors will be evaluated taking into account their dependencies.
	 */
	public void evaluate() {

		// Breadth-first
		// The strategy is to start from completely independent columns (does it include all canEvaluate columns or only having no deps).
		// Then evaluate them, add to finished and find next layer of columns which canEvaluate.
		// canEvaluate takes into account the ability, that is, the conditions: all deps are clean (isChanged inherited), no errors (translate, cycles, inherited)
		// needEvaluate = isChanged

		// The opposite approach is to start from the goal(s) and then evaluate recursively or find layers recirsively.
		// If this column canEvaluate then do it (and return to the previous layer, maybe the initial/last one).
		// If it cannot be evaluated because of errors then return without evaluation by storing this error here (inherited error).
		// If it cannot be evaluated because of dirty dependencies then get a list of these dirty deps.
		// For each dirty dep in the loop, call the evaluate method, which will check if its canEvaluate is because of dirty deps (or errors).
		// If it is because of dirty deps then it will try to evaluate them.
		// If all dirty deps are evaluated, then return success, otherwise return failure which will mean that the previous will also return failure.

		List<Column> done = new ArrayList<>();
		for(List<Column> cols = this.getStartingColumns(); cols.size() > 0; cols = this.getNextColumnsEvaluatable(done)) { // Loop on expansion layers of dependencies forward
			for(Column col : cols) {
				if(!col.isDerived()) continue;
				// TODO: Detect also evaluate errors that could have happened before in this same evaluate loop and prevent this column from evaluation
				// Evaluate errors have to be also taken into account when generating next layer of columns
				BistroError de = col.getError();
				if(de == null || de.code == BistroErrorCode.NONE) {
					col.evaluate();
				}
			}
			done.addAll(cols);
		}

	}
	
	//
	// Dependency graph (needed to determine the order of column evaluations, generated by translation)
	//
	
	protected List<Column> getStartingColumns() { // Return all columns which do not depend on other columns (starting nodes in the dependency graph)
		List<Column> res = this.columns.stream().filter(x -> x.isStartingColumn()).collect(Collectors.<Column>toList());
		return res;
	}
	protected List<Column> getNextDependencies(Column col) { // Return all columns have the specified column in their dependencies (but can depend also on other columns)
		List<Column> res = this.columns.stream().filter(x -> x.getDependencies() != null && x.getDependencies().contains(col)).collect(Collectors.<Column>toList());
		return res;
	}
	protected List<Column> getNextColumns(List<Column> previousColumns) { // Get columns with all their dependencies in the specified list
		List<Column> ret = new ArrayList<>();
		
		for(Column col : this.columns) {

			if(previousColumns.contains(col)) continue; // Already in the list. Ccan it really happen without cycles?
			List<Column> deps = col.getDependencies();
			if(deps == null) continue; // Something wrong

			if(previousColumns.containsAll(deps)) { // All column dependencies are in the list
				ret.add(col); 
			}
		}
		
		return ret;
	}
	protected List<Column> getNextColumnsEvaluatable(List<Column> previousColumns) { // Get columns with all their dependencies in the specified list and having no translation errors (own or inherited)
		List<Column> ret = new ArrayList<>();
		
		for(Column col : this.columns) {
			if(previousColumns.contains(col)) continue;  // Already in the list. Ccan it really happen without cycles?
			List<Column> deps = col.getDependencies();
			if(deps == null) continue; // Something wrong

			// If it has errors then exclude it (cannot be evaluated)
			if(col.hasErrors()) {
				continue;
			}

			// If one of its dependencies has errors then exclude it (cannot be evaluated)
			Column errCol = deps.stream().filter(x -> x.hasErrors()).findAny().orElse(null);
			if(errCol != null) continue;
			
			if(previousColumns.containsAll(deps)) { // All deps have to be evaluated (non-dirty)
				ret.add(col); 
			}
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
	
	public Schema(String name) {
		this.id = UUID.randomUUID();
		this.name = name;
		
		// Create primitive tables
		Table doubleType = createTable("Double");
		Table stringType = createTable("String");
	}

}
