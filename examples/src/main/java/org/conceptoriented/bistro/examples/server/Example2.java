package org.conceptoriented.bistro.examples.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.ExUtils;
import org.conceptoriented.bistro.server.*;
import org.conceptoriented.bistro.server.actions.*;
import org.conceptoriented.bistro.server.connectors.*;

/**
 * Feed bitcoin quotes to the server by storing them as records of one table with timestamp, price and amount.
 * <p>
 * Compute two moving averages as two derived columns of the quote table defined via rolling aggregation with different (sliding) window sizes.
 * For simplicity, use simple aggregation (without smoothing) and window length defined via row number (not durations).
 * <p>
 * The goal is to find unusual events by comparing different moving averages.
 * In this example, we compare the difference between fast and slow moving averages.
 * If the fast average significantly differs from the slow one then we consider it as an anomaly and print a message.
 */
public class Example2 {

    public static String location = "src/main/resources/ds4";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        // Time for the server to run (after that it will stop so it has to be enough for all events).
        long serverProcessingTime = 20000;

        // 46549 events for 1 month. We want to play them for 1 minute, which is approximately 775 events per second
        double acceleration = 31*24*60;

        // This number of records will be kept in the table
        long windowSize = 100;

        //
        // Create schema
        //
        schema = new Schema("Example 2");

        //
        // Create tables
        //
        Table quotes = schema.createTable("QUOTES");

        //
        // Create columns
        //

        //Column time = schema.createColumn("Time", quotes);
        Column time = schema.createColumn("Time", quotes);
        Column price = schema.createColumn("Price", quotes);
        Column amount = schema.createColumn("Amount", quotes);

        // Moving average of the weighted price
        Column avg10 = schema.createColumn("Avg10", quotes);
        avg10.setDefaultValue(0.0); // It will be used as an initial value
        avg10.roll(
                10, 0, // Window definition: 10 previous records
                (a,d,p) -> (double)a + (Double.valueOf((String)p[0]) / 10.0), // This accumulate function will be called for each element in the window
                price
        );

        // Moving average of the weighted price
        Column avg50 = schema.createColumn("Avg50", quotes);
        avg50.setDefaultValue(0.0); // It will be used as an initial value
        avg50.roll(
                50, 0, // Window definition: 50 previous records
                (a,d,p) -> (double)a + (Double.valueOf((String)p[0]) / 50.0), // This accumulate function will be called for each element in the window
                price
        );

        //
        // Create server
        //

        Server server = new Server(schema);

        //
        // Create input connector (simulator)
        //

        ConnectorSimulatorFile simulator = null;
        try {
            simulator = new ConnectorSimulatorFile(server, quotes, location +"/.krakenEUR.csv", "Time", acceleration);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        simulator.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );

        simulator.addAction(
                new ActionEval(schema))// Evaluate after each added quote
        ;

        MyAction2 myAction = new MyAction2(quotes, avg10, avg50);
        simulator.addAction(
                myAction // Check criteria of alert and print if detected using a custom action
        );

        simulator.addAction(
                new ActionRemove(quotes, windowSize) // Retention policy: delete records
        );

        //
        // Create a connector for periodically printing the state
        //

        ConnectorTimer timer = new ConnectorTimer(server,1000);
        timer.addAction(
                x -> {
                    long len = quotes.getIdRange().end;
                    System.out.print("o " + len + "\n");
                }
        );

        //
        // Start the server
        //

        try {
            server.start();

            ExUtils.waitToSecond();
            simulator.start();
            timer.start();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("Server started.");

        try {
            Thread.sleep(serverProcessingTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // All connectors have to be stopped.
        try {
            timer.stop();
            simulator.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("");
        System.out.println("Server stopped.");


        if(myAction.alerts.size() != 16) System.out.println(">>> UNEXPECTED RESULT.");

        // First alert
        if((long)myAction.alerts.get(0).get(0) != 46204) System.out.println(">>> UNEXPECTED RESULT.");
        if(Math.abs((double)myAction.alerts.get(0).get(1) - 403.617576) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
        if(Math.abs((double)myAction.alerts.get(0).get(2) - 406.0798512000001) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");

        // Last alert
        if((long)myAction.alerts.get(15).get(0) != 46219) System.out.println(">>> UNEXPECTED RESULT.");
        if(Math.abs((double)myAction.alerts.get(15).get(1) - 402.78532400000006) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
        if(Math.abs((double)myAction.alerts.get(15).get(2) - 405.2549588000002) > 1e-10) System.out.println(">>> UNEXPECTED RESULT.");
    }

}

class MyAction2 implements Action {

    // Deviation of the fast line from the slow line
    static double deviation = 0.006;

    long lastEnd = 0;

    Table table;
    Column column1; // Fast
    Column column2; // Slow

    public List<List<Object>> alerts = new ArrayList<>();

    @Override
    public void evaluate(Context ctx) throws BistroError {

        long end = this.table.getIdRange().end;

        for( ; lastEnd < end; lastEnd++) {

            double fast = (double)this.column1.getValue(lastEnd);
            double slow = (double)this.column2.getValue(lastEnd);

            if(fast < (1.0 - deviation)*slow) {
                System.out.print(">>> " + lastEnd + ": " + fast + " - " + slow + "\n");
                alerts.add(Arrays.asList(lastEnd, fast, slow));
            }
        }
    }

    public MyAction2(Table table, Column column1, Column column2) {
        this.table = table;
        this.column1 = column1;
        this.column2 = column2;
    }
}
