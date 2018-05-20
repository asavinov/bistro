package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.ExUtils;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.*;
import org.conceptoriented.bistro.server.connectors.*;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Use one timer which wakes up every 1 second and executes sequentially several actions:
 *
 * <ul>
 * <li>The first action explicitly adds one record to the table with random value.
 * <li>The second action deletes old records by keeping the number of records constant.
 * <li>The third action prints the state of the table.
 * <ul/>
 *
 * Use Ctrl-C to stop the server.
 */
public class Example1 {

    public static Schema schema;

    public static void main(String[] args) throws IOException, BistroError {

        // Time for the server to run (after that it will stop so it has to be enough for all events).
        long serverProcessingTime = 10000;

        //
        // Create schema
        //
        Schema schema = new Schema("My Schema");

        Table table = schema.createTable("EVENTS");
        Column column1 = schema.createColumn("Message", table);
        Column column2 = schema.createColumn("Temperature", table);

        //
        // Create and start server
        //
        Server server = new Server(schema);

        //
        // Create connector
        //
        ConnectorTimer timer = new ConnectorTimer(server,1000); // Do something every 1 second

        timer.addAction( // Add one record
                x -> {
                    long id = table.add();
                    double value = ThreadLocalRandom.current().nextDouble(30.0, 40.0);
                    column1.setValue(id, "Hello Bistro Streams!");
                    column2.setValue(id, value);
                }
        );

        timer.addAction( // Keep only 5 latest events
                new ActionRemove(table, 5)
        );

        timer.addAction( // Print something
                x -> {
                    System.out.println("Number of events: " + table.getLength());
                }
        );

        //
        // Start server and connectors
        //

        server.start();

        ExUtils.waitToSecond();
        timer.start();

        System.out.println("Server started. Press Ctrl-C to stop.");

        //
        // Add shutdown hook
        //

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                timer.stop();
                server.stop();
                System.out.println("Server stopped.");
            }
            catch (Exception e) {
                System.out.println("Errors in shutdown hook: " + e);
                System.exit(1) ;
            }
        }));
    }
}
