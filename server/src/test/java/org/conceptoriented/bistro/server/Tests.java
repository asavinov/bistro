package org.conceptoriented.bistro.server;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.actions.*;
import org.conceptoriented.bistro.server.connectors.*;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    protected String resource2path(String resource, String resource2) throws URISyntaxException {

        /*
        Path currentRelativePath = Paths.get("");
        String ss = currentRelativePath.toAbsolutePath().toString();
        System.out.println("Current relative path is: " + ss);

        System.out.println("Current relative path is: " + Paths.get(".").toAbsolutePath().normalize().toString());

        //java.io.InputStream is = this.getClass().getResourceAsStream("../../.krakenEUR.csv");

        //java.io.InputStream is = classLoader.getResourceAsStream("../../test/resources/.krakenEUR.csv");
        */

        ClassLoader classLoader = getClass().getClassLoader();

        java.net.URL url = classLoader.getResource(resource);
        File file;
        if(url != null) {
            java.net.URI uri = url.toURI();
            file = new File(uri);
        }
        else {
            file = new File(resource2);
        }

        String path = file.getAbsolutePath();

        return path;
    }



    @Test
    public void timerTest()
    {
        // Create schema
        Schema schema = new Schema("My Schema");
        Table table = schema.createTable("T");
        Column c = schema.createColumn("C", table);

        Server server = new Server(schema);

        //
        // Create necessary connectors
        //

        // Create a timer action which will add a constant message to the table
        String message = "Hello Server (from Timer)!";
        ConnectorTimer timer = new ConnectorTimer(server,200);
        timer.addAction(
                x -> {
                    long id = table.add();
                    c.setValue(id, message);
                }
        );

        //
        // Run the server
        //

        try {
            server.start();
            timer.start();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error starting server.");
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while waiting for the server to do its work.");
        }

        // All connectors have to be stopped.
        try {
            timer.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error stopping server.");
        }

        assertEquals(2L, table.getLength() );
        assertEquals(message, c.getValue(0));
    }

    @Test
    public void simulatorTest() {
        // Create schema
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column time = s.createColumn("Time", t);
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        // Calculate column
        Column tc = s.createColumn("C", t);
        tc.calculate(
                p -> (Double)p[0] + (Double)p[1],
                ta, tb
        );

        Server server = new Server(s);

        //
        // Prepare connectors
        //

        List<Duration> timestamps = Arrays.asList(
                Duration.ofMillis(100),
                Duration.ofMillis(0), // Merge it with the next record(s)
                Duration.ofMillis(100),
                Duration.ofMillis(200)
        );
        List<Object[]> data = Arrays.asList(
                new Object[] { 1.0, 2.0 },
                new Object[] { 3.0, 4.0 },
                new Object[] { 5.0, 6.0 },
                new Object[] { 7.0, 8.0 }
                );

        Connector connector = new ConnectorSimulator(server, t, timestamps, data);

        //
        // Run the server
        //

        try {
            server.start();
            connector.start();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error starting server.");
        }

        try {
            Thread.sleep(500);

            server.submit(new ActionEval(s)); // Send evaluation task manually

            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while waiting for the server to do its work.");
        }

        // All connectors have to be stopped.
        try {
            connector.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error stopping server.");
        }

        assertEquals(4L, t.getLength() );

        assertEquals(3.0, tc.getValue(0));
        assertEquals(7.0, tc.getValue(1));
        assertEquals(11.0, tc.getValue(2));
    }

    @Test
    public void simulatorFileTest() {
        // Create schema
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column time = s.createColumn("Time", t);
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        // Calculate column
        Column tc = s.createColumn("C", t);
        tc.calculate(
                p ->  Double.valueOf((String)p[0]) * Double.valueOf((String)p[1]),
                ta, tb
        );

        Server server = new Server(s);

        //
        // Prepare connectors
        //

        ConnectorSimulatorFile connector = null;
        try {
            // Relative to root package in src: "../../test/resources/.krakenEUR.csv"
            // Relative to project: "src/test/resources/.krakenEUR.csv"
            String path = resource2path("../../test/resources/.krakenEUR.csv", "src/test/resources/.krakenEUR.csv");
            connector = new ConnectorSimulatorFile(server, t, path, "Time", 3600.0);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        connector.setConverter( x -> Instant.ofEpochSecond(Long.valueOf(x)) );

        //
        // Run the server
        //

        try {
            server.start();
            connector.start();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error starting server.");
        }

        try {
            Thread.sleep(1500);

            server.submit(new ActionEval(s)); // Send evaluation task manually

            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while waiting for the server to do its work.");
        }

        // All connectors have to be stopped.
        try {
            connector.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error stopping server.");
        }

        assertEquals(21L, t.getLength() );

        assertEquals(9.9999991221206, tc.getValue(0));
        assertEquals(3.9708022, tc.getValue(1));
        assertEquals(197.61398, tc.getValue(19));
        assertEquals(633.6320000000001, tc.getValue(20));
    }

    @Test
    public void customProducerTest() throws BistroError {
        // Create schema
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        // Calculate column
        Column tc = s.createColumn("C", t);
        tc.calculate(
                p -> (Double)p[0] + (Double)p[1],
                ta, tb
        );

        Server server = new Server(s);
        server.start();

        ProducingConnector producer = new ProducingConnector(server, t);
        producer.start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while waiting for the server to do its work.");
        }

        // All connectors have to be stopped.
        producer.stop();
        server.stop();

        assertEquals(2L, t.getLength() );

        assertEquals(7.0, tc.getValue(1));
        assertEquals(11.0, tc.getValue(2));
    }

}


class ProducingConnector extends ConnectorBase implements Runnable {

    protected Table table; // We want to add records to this table
    Column ca;
    Column cb;
    Map<Column, Object> record = new HashMap<>();

    @Override
    public void run() {
        ProducingConnector thiz = ProducingConnector.this;
        long delay = 100;

        // Add record 1
        thiz.record.put(ca, 1.0);
        thiz.record.put(cb, 2.0);
        thiz.server.submit(new ActionAdd(thiz.table, thiz.record));

        thiz.server.submit(new ActionEval(thiz.table.getSchema()));

        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while sending events.");
        }

        // Add record 2
        thiz.record.put(ca, 3.0);
        thiz.record.put(cb, 4.0);
        thiz.server.submit(new ActionAdd(thiz.table, thiz.record));

        thiz.server.submit(new ActionEval(thiz.table.getSchema()));

        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while sending events.");
        }

        // Add record 3
        thiz.record.put(ca, 5.0);
        thiz.record.put(cb, 6.0);
        thiz.server.submit(new ActionAdd(thiz.table, thiz.record));

        thiz.server.submit(new ActionEval(thiz.table.getSchema()));

        // Leave only 2 records in the table
        thiz.server.submit(new ActionRemove(thiz.table, 2));
    }

    @Override
    public void start() throws BistroError {
        // Start a thread which will emit one action with certain frequency
        new Thread(this).start();
    }

    public ProducingConnector(Server server, Table table) {
        super(server);
        this.table = table;

        List<Column> columns = this.table.getColumns();
        this.ca = columns.get(0);
        this.cb = columns.get(1);

        this.record.put(ca, null);
        this.record.put(cb, null);
    }
}
