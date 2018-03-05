package org.conceptoriented.bistro.server;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.actions.*;

import java.time.Duration;

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
        // Create necessary actions
        //

        // Create a timer action
        Action timer = new ActionTimer(server,200);

        // Create action for adding a constant event
        String message = "Hello Server (from Timer)!";
        Runnable task = () -> {
            long id = table.add();
            c.setValue(id, message);
        };
        timer.setLambda(task);

        server.addAction(timer);

        //
        // Run the server
        //

        // All actions have to be initialized
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

        // All actions have to be stopped.
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
