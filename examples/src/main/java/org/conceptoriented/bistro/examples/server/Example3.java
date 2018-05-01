package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
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
 * Feed events to the engine into one table.
 * Project the events to the range table with regular intervals and aggregate the quotes over these intervals.
 * Compute several moving averages for the aggregated (projected) time series data.
 * Fire alerts when the fast curve deviates from the slow curve.
 */
public class Example3
{

    public static String location = "src/main/resources/ds4";

    public static Schema schema;

    public static void main(String[] args) throws IOException {

        // 46549 events for 1 month. We want to play them for 1 minute, which is approximately 775 events per second
        double acceleration = 31*24*60;

        // Time processing events by the server (after that it will stop so it has to be enough for all events).
        long serverProcessingTime = 20000;

        // Size of regular intervals for aggregation and analysis
        Duration intervalSize = Duration.ofMillis(1000);

        //
        // Create schema
        //
        schema = new Schema("Example 2");

        //
        // Table with (asynchronous) source quotes
        //

        Table quotes = schema.createTable("QUOTES");

        Column time = schema.createColumn("Time", quotes);
        Column price = schema.createColumn("Price", quotes);
        Column amount = schema.createColumn("Amount", quotes);

        //
        // Table with (synchronous) time intervals
        //

        Table intervals = schema.createTable("INTERVALS");
        Column intervalTime = schema.createColumn("Time", intervals);
        intervalTime.noop(true);
        Column intervalNo = schema.createColumn("Interval", intervals);
        intervalNo.noop(true);

        // Definition
        Instant startInterval = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        intervals.range(
                startInterval,
                intervalSize,
                10000L // Maximum number of intervals
        );

        // Project column pointing to the range table
        Column interval = schema.createColumn("Interval", quotes, intervals);
        interval.project(
                time // Timestamp of the event will be mapped to time ranges
        );

        //
        // Aggregated and rolling columns
        //

        Column volumeSum = schema.createColumn("Volume Sum", intervals);
        volumeSum.setDefaultValue(0.0); // It will be used as an initial value
        volumeSum.accumulate(
                interval, // Link: Even time stamp -> Interval
                (a,p) -> (double)a + Double.valueOf((String)p[0]),
                amount
        );

        Column quoteCount = schema.createColumn("Quote Count", intervals);
        quoteCount.setDefaultValue(0.0); // It will be used as an initial value
        quoteCount.accumulate(
                interval, // Link: Even time stamp -> Interval
                (a,p) -> (double)a + 1.0
        );

        Column avgVolume = schema.createColumn("Average Volume", intervals);
        avgVolume.calculate(
                (p) -> (double)p[0] / (double)p[1],
                volumeSum, quoteCount
        );

        Column avgVolumeIncrease = schema.createColumn("Average Volume Increase", intervals);
        avgVolumeIncrease.setDefaultValue(0.0); // It will be used as an initial value
        avgVolumeIncrease.roll(
                2, 0,
                (a,d,p) -> {
                    if(d == 0) return (double)a + (double)p[0]; // Current interval
                    else if(d == 1) return (double)a - (double)p[0]; // Previous interval
                    else return a; // Should never happen with window size 2
                },
                avgVolume
        );

        //
        // Create server and connectors
        //

        Server server = new Server(schema);

        // Feed data into the schema
        ConnectorSimulatorFile inSimulator = null;
        try {
            inSimulator = new ConnectorSimulatorFile(server, quotes, location +"/.krakenEUR.csv", "Time", acceleration);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        inSimulator.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );

        //
        // Periodically check the state, detect anomalies and print them
        //

        // Periodically check the status
        ConnectorTimer outTimer = new ConnectorTimer(server,500);

        outTimer.addAction(new ActionEval(schema)); // Evaluate

        outTimer.addAction( // Print some status info
                x -> {
                    long len = quotes.getIdRange().end;
                    System.out.print("o " + len + "\n");
                }
        );

        // Detect anomalies and print if found
        MyAction3 myAction = new MyAction3(intervals, avgVolumeIncrease);
        inSimulator.addAction(myAction);

        // Delete old source events and intervals
        outTimer.addAction(new ActionRemove(quotes, 2000));
        outTimer.addAction(new ActionRemove(intervals, 20)); // Note that old intervals will be re-created by projecting old events if they still exist

        //
        // Start the server
        //

        try {
            server.start();

            // Synchronize simulator start with second borders (start with the next second)
            try {
                Instant nextSecond = Instant.now().truncatedTo(ChronoUnit.SECONDS).plusMillis(1100);
                long toNextSecond = Duration.between(Instant.now(), nextSecond).toMillis();
                Thread.sleep(toNextSecond);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            inSimulator.start();
            outTimer.start();
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
            outTimer.stop();
            inSimulator.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
        }
        System.out.println("");
        System.out.println("Server stopped.");


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
    public void evaluate(Context ctx) throws BistroError {

        long end = this.table.getIdRange().end - 1; // Note that we skip the last interval because it is not complete yet

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
