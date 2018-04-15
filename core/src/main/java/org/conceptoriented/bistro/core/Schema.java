package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
        Table ret = this.tables.stream().filter(x -> x.getName().equals(table)).findAny().orElse(null);
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
        List<Column> res = this.columns.stream().filter(x -> x.getInput().getName().equals(table)).collect(Collectors.<Column>toList());
        return res;
    }
    public Column getColumn(Table table, String column) {
        Column ret = this.columns.stream().filter(x -> x.getInput().equals(table) && x.getName().equals(column)).findAny().orElse(null);
        return ret;
    }
    public Column getColumn(String table, String column) {
        Column ret = this.columns.stream().filter(x -> x.getInput().getName().equals(table) && x.getName().equals(column)).findAny().orElse(null);
        return ret;
    }
    public Column getColumnById(String id) {
        Column ret = this.columns.stream().filter(x -> x.getId().toString().equals(id)).findAny().orElse(null);
        return ret;
    }

    public Column createColumn(String name, Table  input) {
        return this.createColumn(name, input, null);
    }
    public Column createColumn(String name, Table  input, Table output) {
        if(output == null) {
            output = this.getTable("Object");
        }
        Column col = new Column(this, name, input, output);
        this.columns.add(col);
        return col;
    }
    public Column createColumn(String name, String input) {
        return this.createColumn(name, input, null);
    }
    public Column createColumn(String name, String input, String output) {
        if(output == null || output.isEmpty()) {
            output = "Object";
        }
        Column col = new Column(this, name, this.getTable(input), this.getTable(output));
        this.columns.add(col);
        return col;
    }
    public void deleteColumn(Column col) {
        this.columns.remove(col);
    }

    //
    // Evaluate
    //

    /**
     * Evaluate all columns of the schema. The order of evaluation is determined by dependencies. Only dirty columns are evaluated. Columns with errors are not evaluated.
     */
    public void evaluate() {
        Topology topology = new Topology(this);
        topology.create();

        this.evaluateToplogy(topology);
    }

    public void evaluate(Column column) {
        Topology topology = new Topology(column);
        topology.create();

        this.evaluateToplogy(topology);
    }

    public void evaluate(Column[] columns) {
    }

    public void evaluate(Table table) {
        Topology topology = new Topology(table);
        topology.create();

        this.evaluateToplogy(topology);
    }

    //
    // Dependency graph
    //

    protected void evaluateToplogy(Topology topology) {

        for(List<Element> layer : topology.layers) {

            for(Element elem : layer) {

                //
                // Check possibility to evaluate
                //
                elem.getExecutionErrors().clear();
                if(elem.hasExecutionErrorsDeep()) { // Columns with evaluation errors (might appear during previous pass) cannot be evaluated and remain dirty
                    continue;
                }
                if(elem.hasDefinitionErrorsDeep()) { // Columns with definition errors cannot evaluated (including cycles)
                    continue;
                }

                //
                // Check need to evaluate
                //
                if(!elem.isDirty()) {
                    continue;
                }

                //
                // Really evaluate
                //
                elem.evaluate();
            }
        }

        // After evaluating, clear the changes in all elements of the graph
        for(List<Element> layer : topology.layers) {
            for(Element elem : layer) {
                elem.resetChanged();
            }
        }
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
        Table objectType = createTable("Object");
    }

}

