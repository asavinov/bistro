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
    public void eval() {
        for(Column col : this.columns) {
            col.getExecutionErrors().clear();
        }

        List<List<Element>> layers = buildLayers();

        for(List<Element> layer : layers) {

            for(Element elem : layer) {

                // This status will be cleared after evaluation so we explicitly propagate it to the dependents
                if(elem.isDirty()) {
                    elem.getDependants().forEach(x -> x.setDirty());
                }

                if(elem.hasDefinitionErrorsDeep()) { // Columns with definition errors cannot evaluated (including cycles)
                    continue;
                }

                if(elem.hasExecutionErrorsDeep()) { // Columns with evaluation errors cannot be evaluated and remain dirty
                    continue;
                }

                // Check that all dependencies are clean (if at one is dirty then we cannot evaluate this)
                boolean oneDepDirty = false;
                for(Element dep : elem.getDependencies()) {
                    if(dep.isDirtyDeep()) { oneDepDirty = true; break; }
                }
                if(oneDepDirty) continue;

                //
                // Really evaluate (and make up-to-date)
                //
                elem.run();
            }

        }

    }

    /**
     * Evaluate one column and dependencies if necessary
     */
    public void eval(Column column) {

        // Evaluate dependencies recursively
        //for(List<Element> deps = column.getDependencies(); deps.size() > 0; deps = column.getDependencies(deps)) {
        //    for(Element dep : deps) {
        //        dep.run();
        //    }
        //}

        column.run();
    }

    /**
     * Evaluate one column and dependencies if necessary
     */
    public void eval(Column[] columns) {
        for(Column column : columns) {
            column.eval();
        }
    }

    /**
     * Evaluate all columns of one table
     */
    public void eval(Table table) {
        for(Column column : table.getColumns()) {
            column.eval();
        }
    }

    //
    // Dependency graph
    //

    protected List<List<Element>> buildLayers() { // Each layer is a list of elements which depend on elements of previous layers

        List<List<Element>> layers = new ArrayList<>();

        List<Element> all = new ArrayList<>();
        all.addAll(this.getColumns());
        all.addAll(this.getTables());

        List<Element> done = new ArrayList<>();

        while(true) { // One pass for each new (non-empty) layer

            List<Element> layer = new ArrayList<>();

            for(Element elem : all) {
                if(done.contains(elem)) continue;
                if(!elem.getDefinitionErrors().isEmpty()) continue; // Do not add to done so dependants are also excluded from the graph

                boolean isNext = true;
                for(Element dep : elem.getDependencies()) {
                    if(!done.contains(dep)) { isNext = false; break; }
                }
                if(isNext) layer.add(elem);
            }

            if(layer.isEmpty()) break;

            layers.add(layer);
            done.addAll(layer);
        }

        return layers;
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
