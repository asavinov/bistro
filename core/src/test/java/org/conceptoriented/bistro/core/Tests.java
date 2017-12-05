package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void schemaTest() // Schema operations
    {
        // Create and configure: schema, tables, keyColumns
        Schema s = new Schema("My Schema");

        Table t = s.createTable("T");

        Column c1 = s.createColumn("A", t);
        Column c2 = s.createColumn("B", t);
        Column c3 = s.createColumn("C", t);

        s.deleteColumn(c2);
        assertEquals(2, t.getColumns().size());

        s.deleteTable(t);
        assertEquals(1, s.getTables().size()); // Only primitive tables
    }

    @Test
    public void dataTest() { // Manual operations table id ranges
        // Prepopulate table for experiments with column data
        Schema s = new Schema("My Schema");
        Table t = s.createTable("My Table");
        Column c1 = s.createColumn("My Column", t);

        long id = t.add();
        assertEquals(0, id);
        long id2 = t.add();
        c1.setValue(id2, 1.0);
        assertEquals(1.0, (double) c1.getValue(id2), Double.MIN_VALUE);

        Column c2 = s.createColumn("My Column 2", t);
        long id3 = t.add();
        c2.setValue(id3, "StringValue");
        assertEquals("StringValue", (String) c2.getValue(id3));

        assertEquals(0, t.getIdRange().start);
        assertEquals(3, t.getIdRange().end);

        // Records.
        // Working with multiple keyColumns (records)
        long id4 = t.add();
        List<Column> cols = Arrays.asList(c1, c2);
        List<Object> vals = Arrays.asList(2.0, "StringValue 2");
        t.setValues(id4, cols, vals);
        assertEquals(2.0, (double) c1.getValue(id4), Double.MIN_VALUE);
        assertEquals("StringValue 2", (String) c2.getValue(id4));

        // Record search
        vals = Arrays.asList(2.0, "StringValue 2");
        long found_id = t.find(vals, cols, false); // Record exist
        assertEquals(id4, found_id);

        vals = Arrays.asList(2.0, "Non-existing value");
        found_id = t.find(vals, cols, false); // Record does not exist
        assertTrue(found_id < 0);

        vals = Arrays.asList(5.0, "String value 5"); // Record does not exist
        found_id = t.find(vals, cols, true); // Add if not found
        assertEquals(4, found_id);

        vals = Arrays.asList(5L, "String value 5"); // Record exist but we specify different type (integer instead of double)
        found_id = t.find(vals, cols, false);
        assertTrue(found_id < 0); // Not found because of different types: Long is not comparable with Double
    }

}
