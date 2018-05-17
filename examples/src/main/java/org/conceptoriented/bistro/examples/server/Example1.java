package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.*;
import org.conceptoriented.bistro.server.connectors.*;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Hello Bistro Streams.
 * Create server and feed it with constant events.
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
        server.start();
        System.out.println("Server started.");

        //
        // Create and start connector
        //
        ConnectorTimer timer = new ConnectorTimer(server,1000); // Do something every 1 second
        timer.addAction(
                x -> {
                    long id = table.add();
                    double value = ThreadLocalRandom.current().nextDouble(30.0, 40.0);
                    column1.setValue(id, "Hello Bistro Streams!");
                    column2.setValue(id, value);
                }
        );
        timer.addAction(new ActionRemove(table, 5)); // Keep only 5 latest events
        timer.addAction(
                x -> {
                    System.out.println("Number of events: " + table.getLength());
                }
        );

        timer.start();

        try {
            Thread.sleep(serverProcessingTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        timer.stop();
        server.stop();
        System.out.println("Server stopped.");
    }

}
