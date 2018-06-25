package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.examples.ExUtils;
import org.conceptoriented.bistro.server.Action;
import org.conceptoriented.bistro.server.Context;
import org.conceptoriented.bistro.server.Server;
import org.conceptoriented.bistro.server.actions.ActionEval;
import org.conceptoriented.bistro.server.actions.ActionRemove;
import org.conceptoriented.bistro.server.connectors.ConnectorSimulatorFile;
import org.conceptoriented.bistro.server.connectors.ConnectorTimer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Feed bitcoin quotes to the server by storing them as records of one table with timestamp, price and amount.
 * <p>
 * We do not analyze quotes in the quote table.
 * Instead, we want to aggregate them for fixed time intervals which are represented as elements of the second table.
 * Each record of this table represents one time interval (tumbling window).
 * Then we project each quote to an element of this table with tumbling windows by creating a link column.
 * This link column is used then to perform aggregation of quotes.
 * <p>
 * Aggregate quotes for each time interval (tumbling window) by computing average volume.
 * This is done by defining accumulate columns.
 * <p>
 * Now we can analyze data aggregated over tumbling windows.
 * Our goal is to find large changes of the volume by comparing the current volume with the previous volume.
 * This is done by defining a rolling aggregation column with window size 2 which essentially will aggregate two records.
 * The result of such rolling aggregation is essentially is a difference function of the average volume column.
 * <p>
 * In order to detect anomalous volume changes we execute a custom action.
 * This action simply compares the value of the difference column with the threshold.
 * It is important however that the last tumbling window contains incomplete aggregations (because new quotes can still be added).
 * Therefore, we ignore this incomplete time interval and use the previous window for detecting anomalies in volume.
 */
public class Example3 {

    public static String location = "src/main/resources/ds4";

    public static Schema schema;

    public static void main(String[] args) {

        // 46549 events for 1 month. We want to play them for 1 minute, which is approximately 775 events per second
        double acceleration = 31*24*60;

        // Time for the server to run (after that it will stop so it has to be enough for all events).
        long serverProcessingTime = 20000;

        //
        // Create schema
        //
        schema = new Schema("Example 3");

        //
        // Table with (asynchronous) source quotes
        //

        Table quotes = schema.createTable("QUOTES");

        Column time = schema.createColumn("Time", quotes);
        Column price = schema.createColumn("Price", quotes);
        Column amount = schema.createColumn("Amount", quotes);

        //
        // Table with (synchronous) time intervals (each record is one tumbling window)
        //

        Table intervals = schema.createTable("INTERVALS");
        Column intervalTime = schema.createColumn("Time", intervals);
        intervalTime.noop(true);
        Column intervalNo = schema.createColumn("Interval", intervals);
        intervalNo.noop(true);

        // Range table. Each record will represent an interval in time of fixed length
        intervals.range(
                Instant.now().truncatedTo(ChronoUnit.SECONDS), // Start interval: snap raster to seconds
                Duration.ofMillis(1000), // Tumbling window size. Time interval for aggregation
                10000L // Maximum number of intervals
        );

        // Project column: each quote record belongs to some interval (tumbling window)
        Column interval = schema.createColumn("Interval", quotes, intervals);
        interval.project(
                time // Timestamp of the event will be mapped to a time interval (window)
        );

        //
        // Aggregation for each time interval (tumbling window)
        //

        Column volumeSum = schema.createColumn("Volume Sum", intervals);
        volumeSum.setDefaultValue(0.0); // It will be used as an initial value
        volumeSum.accumulate(
                interval, // Link: Event time stamp -> Interval
                (a,p) -> (double)a + Double.valueOf((String)p[0]),
                null,
                amount
        );

        Column quoteCount = schema.createColumn("Quote Count", intervals);
        quoteCount.setDefaultValue(0.0); // It will be used as an initial value
        quoteCount.accumulate(
                interval, // Link: Event time stamp -> Interval
                (a,p) -> (double)a + 1.0,
                null
        );

        Column avgVolume = schema.createColumn("Average Volume", intervals);
        avgVolume.calculate(
                (p) -> (double)p[0] / (double)p[1],
                volumeSum, quoteCount
        );

        //
        // Rolling aggregation for two tumbling windows
        //

        Column avgVolumeIncrease = schema.createColumn("Average Volume Increase", intervals);
        avgVolumeIncrease.setDefaultValue(0.0); // It will be used as an initial value
        avgVolumeIncrease.roll(
                2, 0, // Window size: 2 records - this one and the previous one (to compute difference)
                (a,d,p) -> {
                    if(d == 0) return (double)a + (double)p[0]; // Current interval
                    else if(d == 1) return (double)a - (double)p[0]; // Previous interval
                    else return a; // Should never happen with window size 2
                },
                avgVolume
        );

        //
        // Create server
        //

        Server server = new Server(schema);

        //
        // Create input connector (simulator)
        //

        // Feed data into the schema
        ConnectorSimulatorFile simulator = null;
        try {
            simulator = new ConnectorSimulatorFile(server, quotes, location +"/.krakenEUR.csv", "Time", acceleration);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        simulator.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );

        MyAction3 myAction = new MyAction3(intervals, avgVolumeIncrease);
        simulator.addAction(
                myAction // Check criteria of alert and print if detected using a custom action
        );

        //
        // Periodically check the state, detect anomalies and print them
        //

        // Periodically check status
        ConnectorTimer timer = new ConnectorTimer(server,500);

        timer.addAction(new ActionEval(schema)); // Evaluate

        timer.addAction( // Print some status info
                x -> {
                    long len = quotes.getIdRange().end;
                    System.out.print("o " + len + "\n");
                }
        );

        // Delete old source events and intervals
        timer.addAction(new ActionRemove(quotes, 2000));
        timer.addAction(new ActionRemove(intervals, 20)); // Note that old intervals will be re-created by projecting old events if they still exist

        //
        // Start the server
        //

        server.start();
        ExUtils.waitToSecond();
        simulator.start();
        timer.start();
        System.out.println("Server and connectors started.");

        try {
            Thread.sleep(serverProcessingTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // All connectors have to be stopped.
        timer.stop();
        simulator.stop();
        server.stop();
        System.out.println("");
        System.out.println("Server and connectors stopped.");

        // Note that currently the simulated data feed is not synchronized and not deterministically
        // mapped (projected) to time intervals of fixed length. Therefore, we cannot produce
        // deterministic tests
    }

}

class MyAction3 implements Action {

    // Threshold for the volume change
    static double deviation = 0.35;

    long lastEnd = 0;

    Table table;
    Column column; // Volume increase

    public List<List<Object>> alerts = new ArrayList<>();

    @Override
    public void evaluate(Context ctx) {

        // Note that we skip the last interval because it is not complete yet
        // We check conditions only on complete intervals
        long end = this.table.getIdRange().end - 1;

        for( ; lastEnd < end; lastEnd++) {

            double value = (double)this.column.getValue(lastEnd);

            if(Math.abs(value) > deviation) {
                System.out.print(">>> " + lastEnd + ": " + value + "\n");
                alerts.add(Arrays.asList(lastEnd, value));
            }
        }
    }

    public MyAction3(Table table, Column column) {
        this.table = table;
        this.column = column;
    }
}
