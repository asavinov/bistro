package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.connectors.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

/**
 * Feed events to the engine by computing several moving averages and firing alerts when they have a certain relationship.
 */
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
        outTimer.addAction(
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


        // PROBLEM:
        // A (standard) connector does some standard action, e.g., Simulator will add events
        // Also, a connector might not have any (standard) action at all, and it is necessary to provide such an action, e.g., timer.
        // The problem is that it is not possible to flexibly define actions for connectors.
        // In particular, what we might need in connector configuration API:
        // - Specify one action to perform (e.g., timer): setAction/setLambda
        // - Specify a sequence of actions to perform: setTask or setActions/Lammbdas
        // - Attach next actions to execute after whatever the connector will do


        // DONE Feed events.
        // Delete old events
        // Evaluate derived columns: several averages (row-based) and alert column.

        // Produce output.
        // How: action, connector, during evaluation task?
        // How to remember what has been printed already? call output logic for each new row added, or remember processed rows in variables within connector?

        // Maybe introduce periodic (timer-based) log with stats or simply print dot like: .....
        // Then if alert is found then print new line with alert:
        // ...
        // Alert: 5.0 < 10.0 < 12.3
        // ......

    }

}
