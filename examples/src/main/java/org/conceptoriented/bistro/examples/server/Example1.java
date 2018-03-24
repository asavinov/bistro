package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.connectors.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

public class Example1
{

    public static String location = "src/main/resources/ds4";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        //
        // Create schema
        //
        schema = new Schema("Example 1");

        //
        // Create tables
        //
        Table quotes = schema.createTable("T");

        //
        // Create columns
        //

        //Column time = schema.createColumn("Time", quotes);
        Column price = schema.createColumn("Price", quotes);
        Column amount = schema.createColumn("Amount", quotes);

        //
        // Create server and connectors
        //
        Server server = new Server(schema);

        // Feed data into the schema
        ConnectorSimulatorFile inSimulator = null;
        double acceleration = 1.0/(31*24*60); // 46549 for 1 month which we want to play for 1 minute, whic is approximately 775 events per second
        try {
            inSimulator = new ConnectorSimulatorFile(server, quotes, location +"/.krakenEUR.csv", "Time", acceleration);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        inSimulator.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );

        // Periodically print current state
        ConnectorTimer outTimer = new ConnectorTimer(server,1000);
        outTimer.setAction(
                x -> System.out.print(".")
        );


        // TODO: Periodically (synchronously) delete OR (alternatively) delete after addition

        // TODO: Periodically (synchronously) evaluate OR (alternatively) evaluate after addition

        // TODO: Periodically (synchronously) output alerts OR (alternatively) alerts after evaluation

        //
        // Start the server
        //

        try {
            server.start();
            inSimulator.start();
            outTimer.start();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("Server started.");

        try {
            Thread.sleep(65000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // All connectors have to be stopped.
        try {
            outTimer.stop();
            inSimulator.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("");
        System.out.println("Server stopped.");

        // TODO: What we want is to feed data into the schema table.
        // Then we want to periodically compute several moving averages of different lengths (roll columns)
        // Ideally, we want to use time-based windows, and also use volume-weighted prices.
        // Once several moving averages have been computed, we periodically apply an aleat rule which is a special column
        // This column will have 1 if moving averages have special relations a1<a2<a3 or so
        // Once it fires, the output connector detects it and prints a messages (alert)
        // Important: we need to delete unnecessary events (which are older than the longest window)
        // We need to immediately produce alert as it is detected (now) and keep track of alerts so that they are not repeated.
        // Instead of printing an alert, we could write them to a file, or to a separate table (which could be written to a file at the end).

        // 1. Evaluate immediately after adding next batch of events, and also immediately generate alerts
        // 2. Evaluate on timer, e.g., every 100ms but note that several alerts can be produced and not the newest ones
        // 3. Print on timer of after evaluation.
        // Printing can be done from a special table with alerts and the printed row is immediately deleted or otherwise marked for deletion (retention policy based on property and not common time window).

        // Step 1. Feed events. Evaluate several averages (row-based) and alert column.
        // !!! Step 2. How to do output? In an action, connector or evaluation? How to remember already processed rows, e.g., call output logic for each new row added, or remember processed rows in variables within connector?

        // Another option: regular aggregation on ranges.
        // Create range table. Compute averages for each range. What about empty intervals?
        // Compute rolling aggregation on range table (row-based)

        // Maybe introduce periodic (timer-based) log with stats or simply print dot like: .....
        // Then if alert is found then print new line with alert:
        // ...
        // Alert: 5.0 < 10.0 < 12.3
        // ......

    }

}
