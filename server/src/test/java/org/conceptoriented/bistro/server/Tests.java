package org.conceptoriented.bistro.server;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void serverTest()
    {
        // Create schema
        Schema schema = new Schema("My Scheam");

        Server server = new Server(schema);

        //
        // Create actions
        //

        // Create a timer action
        Action timer = new ActionTimer(Duration.ofSeconds(1));
        // TODO: Either customize it with the logic or add next action for adding record

        // Create action for adding a constant event

        // Create a listener action (http or kafka or whatever)
        // The logic of its lambda is what to do with incoming events/requests
        // Normally it is addition but we can define it directly here or in the next action.
        // When initialized, it connects to the hub/port and starts listening.
        // Its callback method knows about the server
        // It will submit itself (with no action - maybe only data preparation) with a table add action (as next action) to the server.
        // Alternatively, it can submit only itself, and then it has to be able to add a record

        // Create a table add action
        // When executed, it will read its input JSON, parse it, and add to the specified table

        //
        // All actions have to be initialized
        //
        server.start();



        //
        // All actions have to be freed.
        //
        server.stop();

        assertTrue(true);
    }

}
