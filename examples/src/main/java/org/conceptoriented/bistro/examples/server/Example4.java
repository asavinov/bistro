package org.conceptoriented.bistro.examples.server;

import org.conceptoriented.bistro.core.*;
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
import java.util.*;

/**
 * Article: https://dzone.com/articles/understanding-bistro-streams-counting-clicks-by-re
 *
 * <pre>
 * CLICKS - USERS - REGIONS
 * </pre>
 */
public class Example4 {

    public static int windowLengthSeconds = 10;


    public static Schema schema;

    public static void main(String[] args) {

        Schema schema = new Schema("Example 4");

        //
        // Create regions table
        //

        Table regions = schema.createTable("REGIONS");
        Column regionName = schema.createColumn("Name", regions);
        Column regionClicks = schema.createColumn("Clicks", regions);

        // We need this to be able to project to this table. If this table is pre-populated (and we use link) then it is not needed.
        regions.product();
        regionName.noop(true);

        //
        // Create users table
        //

        Table users = schema.createTable("USERS");
        Column userName = schema.createColumn("Name", users);
        Column userRegion = schema.createColumn("Region", users);

        // Link column: USERS -> REGIONS
        Column userRegionLink = schema.createColumn("Region Link", users, regions);
        userRegionLink.project(
                new Column[] { userRegion },
                regionName
        );

        // Populate
        for(int i=0; i< ClickSimulator.names.size(); i++) {
            users.getData().add();
            String n = ClickSimulator.names.get(i);
            String r = ClickSimulator.users2regions.get(n);
            userName.getData().setValue(i, n);
            userRegion.getData().setValue(i, r);
        }

        //
        // Create click events table
        //

        Table clicks = schema.createTable("CLICKS");
        Column clickTime = schema.createColumn("Time", clicks);
        Column clickUser = schema.createColumn("User", clicks);
        Column clickCount = schema.createColumn("Count", clicks);

        // Link column: CLICKS -> USERS
        Column clickUserLink = schema.createColumn("User Link", clicks, users);
        clickUserLink.link(
                new Column[] { clickUser },
                userName
        );

        //
        // Define accumulate column
        //

        regionClicks.getData().setDefaultValue(0.0);
        regionClicks.accumulate(
                new ColumnPath(clickUserLink, userRegionLink),
                (a,p) -> (double)a + (double)p[0], // Add the clicks when an event is received
                (a,p) -> (double)a - (double)p[0], // Subtract clicks when the event is deleted (is older than window length)
                new ColumnPath(clickCount) // Measure to be aggregated
        );

        //
        // Create and start server
        //

        Server server = new Server(schema);

        //
        // Connectors
        //

        // It will produce events with average delay where one event is produced for one (random) name with random click count
        ClickSimulator simulator = new ClickSimulator(server, clicks, 1000);

        //
        // Timer will perform some actions periodically
        //

        // We will produce report with this frequency
        ConnectorTimer timer = new ConnectorTimer(server,2000);

        // First, leave only records for the last 10 seconds (window length)
        timer.addAction(
                new ActionRemove(clicks, clickTime, Duration.ofSeconds(Example4.windowLengthSeconds))
        );

        // Then evaluate the current state
        // Note that the deleted records (older than 5 seconds) will be processed by the remover lambda
        timer.addAction(
                new ActionEval(schema)
        );

        // Print status info
        timer.addAction(
                x -> {
                    System.out.print("=== Totals for the last " + Example4.windowLengthSeconds + " seconds: ");

                    Range range = regions.getData().getIdRange();
                    for(long i=range.start; i<range.end; i++) {
                        String name = (String) regionName.getData().getValue(i);
                        Double count = (Double) regionClicks.getData().getValue(i);

                        System.out.print(name + " - " + count + " clicks; ");
                    }

                    System.out.print("\n");
                }
        );

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
            }
            catch (Exception e) {
                System.out.println("Errors in shutdown hook: " + e);
                System.exit(1) ;
            }
        }));
    }
}



class ClickSimulator extends ConnectorBase implements Runnable {

    public static Map<String, String> users2regions;
    public static List<String> names;
    public static List<String> regions;
    static {
        users2regions = new HashMap<>();
        users2regions.put("Alice", "Americas");
        users2regions.put("Bob", "Americas");
        users2regions.put("Ivan", "Europas");
        users2regions.put("Max", "Europas");

        names = new ArrayList<>(users2regions.keySet());
        regions = new ArrayList<>(users2regions.values());
    }


    Thread thread;
    Table table; // Add records to this table
    List<Column> columns = new ArrayList<>();

    long averageDelay;

    private Random random = new Random(123456) ;

    @Override
    public void run() {

        try {
            while ( !(Thread.currentThread().isInterrupted()) ) {

                // Randomly choose next name
                int nameNo = random.nextInt(names.size());
                String name = names.get(nameNo);

                // Randomly choose click count for the selected user
                double averageClickCount = 10.0;
                double count = averageClickCount + random.nextGaussian() * 5.0;
                // Cut tails of the distribution
                if(count < 1.0) count = 1.0;
                if(count > averageClickCount * 2.0 - 1) count = averageClickCount * 2.0 - 1;
                count = Math.round(count);

                this.submit(name, count); // Create and submit a record with coordinates

                System.out.println(count + " clicks from: " + name + " - " + this.users2regions.get(name));

                // Randomize time between events (relative to the average time betwen events)
                int randomInterval = (int) averageDelay / 2;
                int randomMillis = random.nextInt(randomInterval);
                Thread.sleep(averageDelay + randomMillis - (randomInterval/2));
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
    public void start() {
        this.columns = this.table.getColumns();
        this.thread = new Thread(this, "ClickSimulator Thread");
        this.thread.start();
    }

    @Override
    public void stop() {
        // Stop streaming
        if(this.thread != null) {
            this.thread.interrupt();
            this.thread = null;
        }
    }

    private void submit(String name, double count) {
        Map<Column, Object> record = new HashMap<>();
        // First, insert timestamp
        record.put(this.columns.get(0), Instant.now());
        record.put(this.columns.get(1), name);
        record.put(this.columns.get(2), count);

        Action action = new ActionAdd(this.table, record);
        Task task = new Task(action, null);
        this.server.submit(task);
    }

    public ClickSimulator(Server server, Table table, long averageDelay) {
        super(server);

        this.table = table;
        columns.add(table.getColumn("Time"));
        columns.add(table.getColumn("User"));
        columns.add(table.getColumn("Count"));

        this.averageDelay = averageDelay;
    }
}
