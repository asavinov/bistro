package org.conceptoriented.bistro.server;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.actions.*;
import org.conceptoriented.bistro.server.connectors.*;

import java.time.Instant;
import java.util.*;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void timerTest()
    {
        // Create schema
        Schema schema = new Schema("My Scheam");
        Table table = schema.createTable("T");
        Column c = schema.createColumn("C", table);

        Server server = new Server(schema);

        //
        // Create necessary connectors
        //

        // Create a timer action which will add a constant message to the table
        String message = "Hello Server (from Timer)!";
        ConnectorTimer timer = new ConnectorTimer(server,200);
        timer.setAction(
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

        assertEquals(1L, table.getLength() );
        assertEquals(message, c.getValue(0));
    }

    @Test
    public void simulatorTest() {
        // Create schema
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column tts = s.createColumn("TS", t);
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        // Calculate column
        Column tc = s.createColumn("C", t);
        tc.calc(
                p -> (Double)p[0] + (Double)p[1],
                ta, tb
        );

        Server server = new Server(s);

        //
        // Prepare connectors
        //

        List<Instant> timestamps = Arrays.asList(
                Instant.parse("2018-01-01T00:00:00.000Z"),
                Instant.parse("2018-01-01T00:00:00.100Z"),
                Instant.parse("2018-01-01T00:00:00.100Z"),
                Instant.parse("2018-01-01T00:00:00.300Z")
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

            server.submit(new TaskEval(s)); // Send evaluation task manually

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
    public void customProducerTest() {
        // Create schema
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        // Calculate column
        Column tc = s.createColumn("C", t);
        tc.calc(
                p -> (Double)p[0] + (Double)p[1],
                ta, tb
        );

        Server server = new Server(s);

        //
        // Create necessary connectors
        //

        ProducingConnector producer = new ProducingConnector(server, t);

        //
        // Run the server
        //

        try {
            server.start();
            producer.start();
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
            producer.stop();
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error stopping server.");
        }

        assertEquals(2L, t.getLength() );

        assertEquals(7.0, tc.getValue(1));
        assertEquals(11.0, tc.getValue(2));
    }

}


class ProducingConnector extends Connector implements Runnable {

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
        thiz.server.submit(new TaskAdd(thiz.table, thiz.record));

        thiz.server.submit(new TaskEval(thiz.table.getSchema()));

        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while sending events.");
        }

        // Add record 2
        thiz.record.put(ca, 3.0);
        thiz.record.put(cb, 4.0);
        thiz.server.submit(new TaskAdd(thiz.table, thiz.record));

        thiz.server.submit(new TaskEval(thiz.table.getSchema()));

        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Interrupted while sending events.");
        }

        // Add record 3
        thiz.record.put(ca, 5.0);
        thiz.record.put(cb, 6.0);
        thiz.server.submit(new TaskAdd(thiz.table, thiz.record));

        thiz.server.submit(new TaskEval(thiz.table.getSchema()));

        thiz.server.submit(new TaskRemove(thiz.table, 1));
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
