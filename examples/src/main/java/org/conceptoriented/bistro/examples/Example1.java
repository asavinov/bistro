package org.conceptoriented.bistro.examples;

import org.conceptoriented.bistro.core.*;

import java.util.Arrays;

public class Example1 {

	public static void main(String[] args) {

        //
        // Create schema
        //

        Schema schema = new Schema("My Schema");

        //
        // Create tables
        //
        Table table;

        Table things = schema.createTable("THINGS");
        Table events = schema.createTable("EVENTS");

        // Primitive tables are used as data types when defining columns
        Table object = schema.getTable("Object");

        //
        // Create columns
        //
        Column column;

        // Each thing has a name
        Column thingName = schema.createColumn("Name", things, object);

        // Each event stores name of the device it was sent from
        Column eventThingName = schema.createColumn("Thing Name", events, object);

        //
        // Data
        //

        // Elements are appended to tables and are identified by an id
        long id;
        id = things.add(); // id = 0
        id = things.add(); // id = 1

        // Data values are stored in columns as objects
        thingName.setValue(0, "fridge");
        thingName.setValue(1, "oven");

        // Values are read from columns as objects given their id
        Object value;
        value = thingName.getValue(0); // value = "fridge"
        value = thingName.getValue(1); // value = "oven"

        //
        // Calculate column
        //

        // This column will compute the thing name length in characters
        Column calc = schema.createColumn("Name Length", things, object);
        calc.calc(
                (p, o) -> ((String)p[0]).length(), // How to compute
                new Column[]{thingName} // Parameters for computing
        );

        //
        // Calculate column
        //

        // Add some event data to aggregate
        events.add(3);
        eventThingName.setValue(0, "oven");
        eventThingName.setValue(1, "fridge");
        eventThingName.setValue(2, "oven");

        // Link column finds its output in the output table
        Column link = schema.createColumn("Thing", events, things);
        link.link(
                new Column[] {thingName}, // Columns to be used for searching (in the type table)
                new Column[] {eventThingName} // Columns providing criteria for search (in this input table)
        );

        //
        // Accumulate column
        //

        Column counts = schema.createColumn("Event Count", things, object);
        counts.accu(
                (p, o) -> (Double)o + 1.0, // How to accumulate/update
                null, // Nothing to aggregate except for counting
                link // How to group/map facts to this table
        );
        counts.setDefaultValue(0.0); // It will be used as an initial value

        //
        // Evaluate and read values
        //

        schema.eval(); // All 3 derived columns will be evaluated

        // Calculate column
        value = calc.getValue(0); // value = 6
        value = calc.getValue(1); // value = 4

        // Link column
        value = link.getValue(0); // value = 1
        value = link.getValue(1); // value = 0
        value = link.getValue(2); // value = 1

        // Accu column
        value = counts.getValue(0); // 1 event from fridge
        value = counts.getValue(1); // 2 events from oven
    }
}
