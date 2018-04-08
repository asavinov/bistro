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
        List<List<Element>> layers = buildLayers();
        this.eval_graph(layers);
    }

    public void eval(Column column) {
        List<List<Element>> layers = buildLayers(column);
        this.eval_graph(layers);
    }

    public void eval(Column[] columns) {
    }

    public void eval(Table table) {
        List<List<Element>> layers = buildLayers(table);
        this.eval_graph(layers);
    }

    //
    // Dependency graph
    //

    protected void eval_graph(List<List<Element>> layers) {

        for(List<Element> layer : layers) {

            for(Element elem : layer) {

                elem.getExecutionErrors().clear();

                // This status will be cleared after evaluation so we explicitly propagate it to the direct dependents (not all of them are in this graph)
                if(elem.isChanged()) {
                    elem.getDependents().forEach(x -> x.setChanged());
                }

                if(elem.hasDefinitionErrorsDeep()) { // Columns with definition errors cannot evaluated (including cycles)
                    continue;
                }

                if(elem.hasExecutionErrorsDeep()) { // Columns with evaluation errors (might appear during previous pass) cannot be evaluated and remain dirty
                    continue;
                }

                // Check that all dependencies are clean (if at one is dirty then we cannot evaluate this)
                boolean dirtyDepFound = false;
                for(Element dep : elem.getDependencies()) {
                    if(dep.isChangedDependencies()) { dirtyDepFound = true; break; }
                }
                if(dirtyDepFound) continue;

                //
                // Really evaluate (and make up-to-date)
                //
                elem.run();
            }

        }

    }

    protected List<List<Element>> buildLayers() { // Build a list of layers of the graph

        List<List<Element>> layers = new ArrayList<>(); // Each layer is a list of elements which depend on elements of previous layers

        List<Element> all = new ArrayList<>();
        all.addAll(this.getColumns());
        all.addAll(this.getTables());

        List<Element> done = new ArrayList<>();

        while(true) { // One pass for each new (non-empty) layer

            List<Element> layer = new ArrayList<>();

            for(Element elem : all) {

                if(done.contains(elem)) continue;

                // Do not add to done so dependants are also excluded from the graph.
                // It is important for avoiding cycles (same element in different layers) because elements in cycles are supposed to have definition error.
                if(!elem.getDefinitionErrors().isEmpty()) continue;

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

    protected List<List<Element>> buildLayers(Element element) { // Build graph with one element as the last element
        List<List<Element>> layers = new ArrayList<>(); // Each layer is a list of elements which depend on elements of previous layers

        // Start from the last layer and then each previous layer will contain all dependencies of the previous layer elements

        List<Element> layer = new ArrayList<>();
        layer.add(element);

        while(true) {

            layers.add(0, new ArrayList<>(layer));
            layer.clear();

            for(Element elem : layers.get(0)) { // All elements of the previous layer
                for(Element dep : elem.getDependencies()){

                    if(layer.contains(dep)) continue;

                    if(!elem.getDefinitionErrors().isEmpty()) continue;

                    layer.add(dep);
                }
            }

            if(layer.isEmpty()) break;
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
