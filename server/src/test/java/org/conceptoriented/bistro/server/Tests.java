package org.conceptoriented.bistro.server;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.actions.*;

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

        server.addAction(timer);

        //
        // Run the server
        //

        // All connectors have to be initialized
        try {
            server.start();
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
            server.stop();
        } catch (BistroError bistroError) {
            bistroError.printStackTrace();
            fail("Error stopping server.");
        }

        assertEquals(1L, table.getLength() );
        assertEquals(message, c.getValue(0));
    }

}
