package org.conceptoriented.bistro.examples.core;

import org.conceptoriented.bistro.core.*;

public class Example1 {

    public static void main(String[] args) {

        //
        // Create schema
        //

        Schema schema = new Schema("Example 1");

        //
        // Create tables
        //
        Table things = schema.createTable("THINGS");
        Table events = schema.createTable("EVENTS");

        // Primitive tables are used as data types when defining columns
        Table objects = schema.getTable("Object");

        //
        // Create columns
        //

        // Each thing has a name
        Column thingName = schema.createColumn("Name", things, objects);

        // Each event stores name of the device it was sent from
        Column eventThingName = schema.createColumn("Thing Name", events, objects);

        //
        // Data
        //

        // Elements are appended to tables and are identified by an id
        long id;
        id = things.getData().add(); // id = 0
        id = things.getData().add(); // id = 1

        // Data values are stored in columns as objects
        thingName.getData().setValue(0, "fridge");
        thingName.getData().setValue(1, "oven");

        // Values are read from columns as objects given their id
        Object value;
        value = thingName.getData().getValue(0); // value = "fridge"
        value = thingName.getData().getValue(1); // value = "oven"

        //
        // Calculate column
        //

        // This column will compute the thing name length in characters
        Column calc = schema.createColumn("Name Length", things, objects);
        calc.calculate(
                p -> ((String)p[0]).length(), // How to compute
                thingName // One parameter to compute the column
        );

        //
        // Calculate column
        //

        // Add some event data to aggregate
        events.getData().add(3);
        eventThingName.getData().setValue(0, "oven");
        eventThingName.getData().setValue(1, "fridge");
        eventThingName.getData().setValue(2, "oven");

        // Link column finds its output in the output table
        Column link = schema.createColumn("Thing", events, things);
        link.link(
                new Column[] { eventThingName }, // Columns providing criteria (values) for search (in this input table)
                thingName // Columns to be used for searching (in the type table)
        );

        //
        // Accumulate column
        //

        Column counts = schema.createColumn("Event Count", things, objects);
        counts.accumulate(
                link, // How to group/map facts to this table
                (a,p) -> (Double)a + 1.0, // How to accumulate/update
                null
                // Nothing to aggregate from facts except for counting
        );
        counts.getData().setDefaultValue(0.0); // It will be used as an initial value

        //
        // Evaluate and read values
        //

        schema.evaluate(); // All 3 derived columns will be evaluated

        // Calculate column
        value = calc.getData().getValue(0); // value = 6
        value = calc.getData().getValue(1); // value = 4
        if(((Number)value).longValue() != 4) System.out.println(">>> UNEXPECTED RESULT.");

        // Link column
        value = link.getData().getValue(0); // value = 1 (id of fridge)
        value = link.getData().getValue(1); // value = 0 (id of oven)
        value = link.getData().getValue(2); // value = 1 (id of fridge)
        if(((Number)value).longValue() != 1) System.out.println(">>> UNEXPECTED RESULT.");

        // Accu column
        value = counts.getData().getValue(0); // 1 event from fridge
        value = counts.getData().getValue(1); // 2 events from oven
        if(((Number)value).longValue() != 2) System.out.println(">>> UNEXPECTED RESULT.");
    }
}
