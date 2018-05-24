package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Schema;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.examples.ExUtils;
import org.conceptoriented.bistro.server.Action;
import org.conceptoriented.bistro.server.ConnectorBase;
import org.conceptoriented.bistro.server.Server;
import org.conceptoriented.bistro.server.Task;
import org.conceptoriented.bistro.server.actions.ActionAdd;
import org.conceptoriented.bistro.server.actions.ActionEval;
import org.conceptoriented.bistro.server.actions.ActionRemove;
import org.conceptoriented.bistro.server.connectors.ConnectorTimer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * A sensor asynchronously sends its (X, Y) coordinates to the server.
 * The goal is to detect anomalies by analyzing its trajectory.
 * <p>
 * The analysis consists of the following main steps:
 * <ul>
 * <li>The events from the sensor are stored in table A´which has X and Y columns as well as a time stamp.
 *
 * <li>The incoming asynchronous data events are aggregated over some raster which is a number of time intervals or tumbling windows.
 * The raster is represented by a range table each record of which represents an interval of fixed length.
 * Table A has a link column which points to the raster table.
 * All events from A will reference some raster interval (tumbling window) it belongs to.
 *
 * <li>Event (X, Y) coordinates are aggregated over tumbling windows (raster intervals).
 * This aggregation over tumbling windows is performed by defining accumulate columns.
 *
 * <li>The behavior of the sensor (trajectory) is analyzed over time using rolling aggregation.
 *
 * <li>Anomalies are detected by comparing computed values with some thresholds and firing alerts.
 * <ul/>
 */
public class Example5 {

    public static Schema schema;

    public static void main(String[] args) throws IOException, BistroError {

        Schema schema = new Schema("Example 5");

        //
        // Create event tables each having a timestamp, X, and Y columns (link derived column will be defined later)
        //
        Table sensor_a = schema.createTable("A");
        Column at = schema.createColumn("T", sensor_a);
        Column ax = schema.createColumn("X", sensor_a);
        Column ay = schema.createColumn("Y", sensor_a);

        //
        // Create range table with time intervals
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

        //
        // Define project columns of event tables. Each event belongs to some interval
        //

        Column ai = schema.createColumn("Interval", sensor_a, intervals);
        ai.project(
                at // Timestamp of the event will be mapped to a time interval (window)
        );

        //
        // Define accumulate columns for aggregating X,Y coordinates over intervals
        //

        Column axSum = schema.createColumn("AX Sum", intervals);
        axSum.setDefaultValue(0.0);
        axSum.accumulate(ai, (a,p) -> (double)a + (double)p[0], null, ax);

        Column aySum = schema.createColumn("AY Sum", intervals);
        aySum.setDefaultValue(0.0);
        aySum.accumulate(ai, (a,p) -> (double)a + (double)p[0], null, ay);

        Column aCount = schema.createColumn("A Count", intervals);
        aCount.setDefaultValue(0.0);
        aCount.accumulate(ai, (a,p) -> (double)a + 1.0, null);

        Column axAvg = schema.createColumn("AX Average", intervals);
        axAvg.calculate((p) -> (double)p[0] / (double)p[1], axSum, aCount);

        Column ayAvg = schema.createColumn("AY Average", intervals);
        ayAvg.calculate((p) -> (double)p[0] / (double)p[1], aySum, aCount);

        //
        // Rolling aggregation to analyze trajectory
        //

        Column axSmoothed = schema.createColumn("AX Smoothed", intervals);
        axSmoothed.setDefaultValue(0.0);
        axSmoothed.roll(
                6, 0, // Window size
                (a,d,p) -> p[0] != null ? (double)a + (double)p[0] / 6.0 : 0.0,
                axAvg
        );

        Column aySmoothed = schema.createColumn("AY Smoothed", intervals);
        aySmoothed.setDefaultValue(0.0);
        aySmoothed.roll(
                6, 0, // Window size
                (a,d,p) -> p[0] != null ? (double)a + (double)p[0] / 6.0 : 0.0,
                ayAvg
        );

        // Deviation of the current (X,Y) from the smoothed (X,Y)
        Column xyDeviation = schema.createColumn("XY Deviation", intervals);
        xyDeviation.calculate(
                (p) -> {
                    if(p[0] == null ||  p[1] == null ||  p[2] == null ||  p[3] == null) return 0.0;
                    double dX = (double)p[2] - (double)p[0];
                    double dY = (double)p[3] - (double)p[1];
                    return Math.sqrt(dX*dX + dY*dY);
                },
                axSmoothed, aySmoothed, axAvg, ayAvg // Parameters
        );

        //
        // Create and start server
        //

        Server server = new Server(schema);

        //
        // Connectors
        //

        SensorSimulator simulator = new SensorSimulator(server, sensor_a, 345, 1000.0, 1.0, 0.001);

        //
        // Periodically check the state, detect anomalies and print them
        //

        ConnectorTimer timer = new ConnectorTimer(server,1000);

        timer.addAction(new ActionEval(schema)); // Evaluate

        timer.addAction( // Print some status info
                x -> {
                    long len = intervals.getIdRange().end;
                    if(len < 2) return;
                    // Latest complete deviation
                    double deviation = (double) xyDeviation.getValue(len - 2);
                    System.out.print("o " + len + ": ");
                    if(deviation > 2.0) {
                        System.out.print(">>> Anomaly detected. Deviation is too large: " + deviation + "\n");
                    }
                    else {
                        System.out.print("Deviation from moving average: " + deviation + "\n");
                    }
                }
        );

        // Delete old events and intervals
        timer.addAction(new ActionRemove(sensor_a, 2000));
        timer.addAction(new ActionRemove(intervals, 20));

        //
        // Start the server
        //

        server.start();

        ExUtils.waitToSecond();
        simulator.start();
        timer.start();

        System.out.println("Server started. Press Ctrl-C to stop.");

        //
        // Add shutdown hook
        //

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                timer.stop();
                simulator.stop();
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


// Based on this project: https://github.com/ashokc/Kafka-Streams-Catching-Data-in-the-Act
class SensorSimulator extends ConnectorBase implements Runnable {

    Thread thread;
    Table table; // Add records to this table
    List<Column> columns = new ArrayList<>();

    //	Y = A * sin (w*t).	w: angular velocity. radinans/second
    //	angularV = 2.0 * Math.PI / 60.0 ;	It will take 60secs i.e. 1 minute to trace the full circle. => period = 1 min (one revolution a minute)

    long sleepTimeMillis;

    private double amplitude;
    private double angularV;
    private double error;

    private Random random = new Random(123456) ;

    @Override
    public void run() {

        double valX = 0.0;
        double valY = 0.0;

        try {
            while ( !(Thread.currentThread().isInterrupted()) ) {

                double rand = -error + random.nextDouble() * 2.0 * error ;
                valX = valX + amplitude * Math.sin(angularV) * rand;
                valY = valY + amplitude * Math.cos(angularV) * rand;

                //System.out.println("Coordinates: " + valX + ", " + valY);
                this.submit(valX, valY); // Create and submit a record with coordinates

                Thread.sleep(sleepTimeMillis) ;
            }
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted: " + e);
        }
        catch (Exception e) {
            System.out.println("Error: " + e);
        }
    }

    @Override
    public void start() throws BistroError {
        this.columns = this.table.getColumns();
        this.thread = new Thread(this, "SensorSimulator Thread");
        this.thread.start();
    }

    @Override
    public void stop() throws BistroError {
        // Stop streaming
        if(this.thread != null) {
            this.thread.interrupt();
            this.thread = null;
        }
    }

    private void submit(double valX, double valY) {
        Map<Column, Object> record = new HashMap<>();
        // First, insert timestamp
        record.put(this.columns.get(0), Instant.now());
        record.put(this.columns.get(1), valX);
        record.put(this.columns.get(2), valY);

        Action action = new ActionAdd(this.table, record);
        Task task = new Task(action, null);
        this.server.submit(task);
    }

    public SensorSimulator(Server server, Table table, long sleepTimeMillis, double amplitude, double angularV, double error) {
        super(server);

        this.table = table;
        columns.add(table.getColumn("T"));
        columns.add(table.getColumn("X"));
        columns.add(table.getColumn("Y"));

        this.sleepTimeMillis = sleepTimeMillis;

        this.amplitude = amplitude;
        this.angularV = angularV * 2.0 * Math.PI / 60.0;
        this.error = error;
    }
}
