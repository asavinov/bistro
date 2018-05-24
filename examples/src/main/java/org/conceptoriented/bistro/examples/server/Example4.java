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
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 *
 * <pre>
 * CLICKS -> USERS -> REGIONS
 * <pre/>
 */
public class Example4 {

    public static Schema schema;

    public static void main(String[] args) throws IOException, BistroError {

        Schema schema = new Schema("Example 5");

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
        users.add();
        userName.setValue(0, ClickSimulator.names.get(0));
        userRegion.setValue(0, ClickSimulator.regions.get(0));
        users.add();
        userName.setValue(1, ClickSimulator.names.get(1));
        userRegion.setValue(1, ClickSimulator.regions.get(1));

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

        regionClicks.setDefaultValue(0.0);
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

        ClickSimulator simulator = new ClickSimulator(server, clicks, 567);

        //
        // Periodically remove old events, evaluate current state and print the results
        //

        ConnectorTimer timer = new ConnectorTimer(server,1000);

        // Retention policy: remove older events
        // TODO: We need table removal method which acts on some date column by removing all records older than the specified values
        //   - actually, it takes any double value and then compares it with the value in the specified column (casted to double) by assuming that they are always ordered
        timer.addAction(new ActionRemove(clicks, 10));

        // Evaluate. Note that the deleted (old) records will be processed by the remover lambda during accumulation
        timer.addAction(new ActionEval(schema));

        // Print status info
        timer.addAction(
                x -> {
                    long len = clicks.getIdRange().getLength();
                    System.out.print("o " + len + ": ");
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
                System.out.println("Server stopped.");
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

    long sleepTimeMillis;

    private Random random = new Random(123456) ;

    @Override
    public void run() {

        try {
            while ( !(Thread.currentThread().isInterrupted()) ) {

                random.nextDouble();

                // Randomly choose next name
                int nameNo = random.nextInt(names.size());
                String name = names.get(nameNo);

                // Randomly choose click count
                double count = random.nextGaussian() * 100 + 50;
                if(count < 1.0) count = 1.0;
                if(count > 100.0) count = 100.0;
                count = Math.round(count);

                this.submit(name, count); // Create and submit a record with coordinates

                // TODO: Randomly choose time to wait
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
        this.thread = new Thread(this, "ClickSimulator Thread");
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

    public ClickSimulator(Server server, Table table, long sleepTimeMillis) {
        super(server);

        this.table = table;
        columns.add(table.getColumn("Time"));
        columns.add(table.getColumn("User"));
        columns.add(table.getColumn("Count"));

        this.sleepTimeMillis = sleepTimeMillis;
    }
}
