package org.conceptoriented.bistro.server;

import org.conceptoriented.bistro.core.BistroError;
import org.conceptoriented.bistro.core.Column;
import org.conceptoriented.bistro.core.Schema;
import org.conceptoriented.bistro.core.Table;
import org.conceptoriented.bistro.server.actions.ActionAdd;
import org.conceptoriented.bistro.server.actions.ActionEval;
import org.conceptoriented.bistro.server.actions.ActionRemove;
import org.conceptoriented.bistro.server.connectors.ConnectorSimulator;
import org.conceptoriented.bistro.server.connectors.ConnectorSimulatorFile;
import org.conceptoriented.bistro.server.connectors.ConnectorTimer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ActionTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void actionAddRemoveTest() throws BistroError, InterruptedException {
        // Create schema
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        Map<Column, Object> r;
        ActionAdd aAdd;
        ActionRemove aRemove;
        Instant now = Instant.now();

        Server server = new Server(s);
        server.start();

        r = new HashMap<>();
        r.put(ta, 1.0); r.put(tb, now.minusSeconds(180));
        aAdd = new ActionAdd(t, r);
        server.submit(aAdd);

        r = new HashMap<>();
        r.put(ta, 2.0); r.put(tb, now.minusSeconds(120));
        aAdd = new ActionAdd(t, r);
        server.submit(aAdd);

        r = new HashMap<>();
        r.put(ta, 3.0); r.put(tb, now.minusSeconds(60));
        aAdd = new ActionAdd(t, r);
        server.submit(aAdd);

        r = new HashMap<>();
        r.put(ta, 4.0); r.put(tb, now);
        aAdd = new ActionAdd(t, r);
        server.submit(aAdd);

        Thread.sleep(50); // Give some time to the server to process actions

        assertEquals(4, t.getLength());

        aRemove = new ActionRemove(t, 10); // Ensure that there are 10 records or less
        server.submit(aRemove);

        Thread.sleep(50); // Give some time to the server to process actions

        assertEquals(4, t.getLength());

        aRemove = new ActionRemove(t, tb, Duration.ofHours(1)); // Ensure that all records are maximum 1 hour old
        server.submit(aRemove);

        Thread.sleep(50); // Give some time to the server to process actions

        assertEquals(4, t.getLength());

        aRemove = new ActionRemove(t, tb, Duration.ofMinutes(2)); // Ensure that all records are maximum 3 minutes old
        server.submit(aRemove);

        Thread.sleep(50); // Give some time to the server to process actions

        assertEquals(2, t.getLength()); // Two record was deleted
    }
}
