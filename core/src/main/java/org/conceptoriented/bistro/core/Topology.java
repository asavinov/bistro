package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

public class Topology {

    Schema schema;

    Element element;

    // Each layer is a list of elements which depend on elements of previous layers
    List<List<Element>> layers = new ArrayList<>();

    public void create() {
        if(this.schema == null) return;

        if(this.element == null) this.create_for_schema();
        else this.create_for_element();
    }

    protected void create_for_schema() { // Build a list of layers of the graph

        this.layers = new ArrayList<>(); // Each layer is a list of elements which depend on elements of previous layers

        List<Element> all = new ArrayList<>();
        all.addAll(this.schema.getColumns());
        all.addAll(this.schema.getTables());

        List<Element> done = new ArrayList<>();

        while(true) { // One pass for each new (non-empty) layer

            List<Element> layer = new ArrayList<>();

            for(Element elem : all) {

                if(done.contains(elem)) continue;

                // Do not add to done so dependents are also excluded from the graph.
                // It is important for avoiding cycles (same element in different layers) because elements in cycles are supposed to have definition error.
                if(!elem.getDefinitionErrors().isEmpty()) continue;

                boolean isNext = true;
                for(Element dep : elem.getDependencies()) {
                    if(!done.contains(dep)) { isNext = false; break; }
                }
                if(isNext) layer.add(elem);
            }

            if(layer.isEmpty()) break;

            this.layers.add(layer);
            done.addAll(layer);
        }
    }

    protected void create_for_element() { // Build graph with one element as the last element

        this.layers = new ArrayList<>(); // Each layer is a list of elements which depend on elements of previous layers

        // Start from the last layer and then each previous layer will contain all dependencies of the previous layer elements

        List<Element> layer = new ArrayList<>();
        layer.add(this.element);

        while(true) {

            this.layers.add(0, new ArrayList<>(layer));
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
    }

    public Topology(Column column) {
        this.element = column;
        this.schema = column.getSchema();
    }
    public Topology(Table table) {
        this.element = table;
        this.schema = table.getSchema();
    }
    public Topology(Schema schema) {
        this.schema = schema;
    }
}
