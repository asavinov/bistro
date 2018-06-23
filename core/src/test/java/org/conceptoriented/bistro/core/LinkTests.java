package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LinkTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void linkTest() {
        Schema s = createSchema();
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");
        Column t2c = t2.getColumn("C");

        // Use column valuePaths
        Column[] valueColumns = new Column[] {t2.getColumn("A"), t2.getColumn("B")};
        Column[] keyColumns = new Column[] {t.getColumn("A"), t.getColumn("B")};

        t2c.link(
                valueColumns, // A and B from T2
                keyColumns // A and B from T
        );
        t2c.evaluate();

        // Check correctness of dependencies
        List<Element> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t2.getColumn("B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(-1L, t2c.getValue(1)); // Not found
    }

    Schema createSchema() {
        Schema s = new Schema("My Schema");

        //
        // Table 1 (type table to link to)
        //
        Table t1 = s.createTable("T");
        t1.product();

        Column t1a = s.createColumn("A", t1);
        t1a.noop(true);
        Column t1b = s.createColumn("B", t1);
        t1b.noop(true);

        // Add one record to link to
        t1.add();
        t1a.setValue(0, 5.0);
        t1b.setValue(0, "bbb");

        //
        // Table 2 (referencing table to link records from)
        //
        Table t2 = s.createTable("T2");

        Column t2a = s.createColumn("A", t2);
        Column t2b = s.createColumn("B", t2);

        Column t2c = s.createColumn("C", t2, t1);

        // Add two records to link from
        t2.add();
        t2a.setValue(0, 5.0);
        t2b.setValue(0, "bbb");
        t2.add();
        t2a.setValue(1, 10.0);
        t2b.setValue(1, "ccc");

        return s;
    }

    //
    // Link and project to number range
    //

    @Test
    public void linkRangeNumberTest() {
        Schema s = createSchemaNumber();
        Table t = s.getTable("R"); // Group range table
        Table f = s.getTable("F"); // Fact table

        Column f_a = f.getColumn("A");

        //
        // Define link column
        //
        Column f_b = f.getColumn("B");
        f_b.link(
                new ColumnPath(f_a) // This column values will be mapped to intervals
        );

        s.evaluate();

        // Check correctness of dependencies
        List<Element> f_c_deps = f_b.getDependencies();
        assertTrue(f_c_deps.contains(f.getColumn("A")));
        assertTrue(f_c_deps.contains(t));

        // Population size
        assertEquals(5L, t.getLength());

        // Check links
        assertEquals(-1L, f_b.getValue(0));
        assertEquals(0L, f_b.getValue(1));
        assertEquals(0L, f_b.getValue(2));
        assertEquals(1L, f_b.getValue(3));
        assertEquals(4L, f_b.getValue(4));
        assertEquals(4L, f_b.getValue(5));
        assertEquals(-1L, f_b.getValue(6));
        assertEquals(-1L, f_b.getValue(7));
    }

    @Test
    public void projRangeNumberTest() {
        Schema s = createSchemaNumber();
        Table t = s.getTable("R"); // Group range table
        Table f = s.getTable("F"); // Fact table

        // Modify operation to allow for more intervals
        t.range(
                10.0, // Origin: unchanged
                20.0, // Step: unchanged
                8L // Count: 8 instead of 5
        );

        // Add more facts to be mapped to non-existing intervals
        Column f_a = f.getColumn("A");
        f_a.setValue(6, 110.0); // 1 interval has to be added [110,130)
        f_a.setValue(7, 160.0); // 2 intervals have to be added [130,150) and [150,170)
        f.add();
        f_a.setValue(8, 260.0); // No intervals have to be added because exceeds the maximum count

        //
        // Define project column
        //
        Column f_b = f.getColumn("B");
        f_b.project(
                new ColumnPath(f.getColumn("A")) // This column values will be mapped to intervals
        );

        s.evaluate();

        // Check correctness of dependencies
        List<Element> f_c_deps = f_b.getDependencies();
        assertTrue(f_c_deps.contains(f.getColumn("A")));
        assertTrue(f_c_deps.contains(t)); // Project column depends on the output (range) table operation

        // Range table does not depend on the incoming project columns and their input table
        List<Element> t_deps = t.getDependencies();
        assertTrue(!t_deps.contains(f_b));
        assertTrue(!t_deps.contains(f));

        // Population size
        assertEquals(8L, t.getLength());

        // Check links
        assertEquals(5L, f_b.getValue(6));
        assertEquals(7L, f_b.getValue(7));
        assertEquals(-1L, f_b.getValue(8));
    }

    Schema createSchemaNumber() { // For links to range table
        Schema s = new Schema("My Schema 2");

        //
        // Table 1 (range table)
        //
        Table t = s.createTable("R");

        Column t_a = s.createColumn("V", t);
        t_a.noop(true);
        Column t_i = s.createColumn("I", t);
        t_i.noop(true);

        t.range(
                10.0, // Origin of the raster
                20.0, // Step of the reaster (interval length)
                5L // How many intervals in the raster
        );

        //
        // Table 2 (referencing table)
        //
        Table f = s.createTable("F");

        Column f_a = s.createColumn("A", f);

        // Add two records to link from
        f.add(8);
        f_a.setValue(0, -200.0); // Too low
        f_a.setValue(1, 10.0); // Exactly very first point
        f_a.setValue(2, 20.0); // Between first and second points
        f_a.setValue(3, 30.0); // Second point
        f_a.setValue(4, 90.0); // Last point
        f_a.setValue(5, 100.0); // After last point but within the last interval
        f_a.setValue(6, 200.0); // Too high
        f_a.setValue(7, Double.NaN); // NaN

        Column f_b = s.createColumn("B", f, t);

        return s;
    }

    //
    // Link and project to duration range
    //

    @Test
    public void linkRangeDurationTest() {
        Schema s = createSchemaDuration();
        Table t = s.getTable("R"); // Group range table
        Table f = s.getTable("F"); // Fact table

        Column f_a = f.getColumn("A");

        //
        // Define link column
        //
        Column f_b = f.getColumn("B");
        f_b.link(
                new ColumnPath(f_a) // This column values will be mapped to intervals
        );

        s.evaluate();

        // Check correctness of dependencies
        List<Element> f_c_deps = f_b.getDependencies();
        assertTrue(f_c_deps.contains(f.getColumn("A")));
        assertTrue(f_c_deps.contains(t));

        // Population size
        assertEquals(2L, t.getLength());

        // Check links
        assertEquals(-1L, f_b.getValue(0));
        assertEquals(0L, f_b.getValue(1));
        assertEquals(0L, f_b.getValue(2));
        assertEquals(1L, f_b.getValue(3));
        assertEquals(1L, f_b.getValue(4));
        assertEquals(1L, f_b.getValue(5));
        assertEquals(-1L, f_b.getValue(6));
        assertEquals(-1L, f_b.getValue(7));
    }

    @Test
    public void projRangeDurationTest() {
        Schema s = createSchemaDuration();
        Table t = s.getTable("R"); // Group range table
        Table f = s.getTable("F"); // Fact table

        // Modify operation to allow for more intervals
        t.range(
                Instant.parse("2018-01-01T00:45:00.00Z"),
                Duration.ofHours(2),
                5L // Count: 5 instead of 2
        );

        // Add more facts to be mapped to non-existing intervals
        Column f_a = f.getColumn("A");
        f_a.setValue(6, Instant.parse("2018-01-01T04:45:00.00Z")); // 1 interval has to be added
        f_a.setValue(7, Instant.parse("2018-01-01T09:45:00.00Z")); // 2 intervals have to be added
        f.add();
        f_a.setValue(8, Instant.parse("2018-01-01T12:45:00.00Z")); // No intervals have to be added because exceeds the maximum count

        //
        // Define project column
        //
        Column f_b = f.getColumn("B");
        f_b.project(
                new ColumnPath(f.getColumn("A")) // This column values will be mapped to intervals
        );

        s.evaluate();

        // Check correctness of dependencies
        List<Element> f_c_deps = f_b.getDependencies();
        assertTrue(f_c_deps.contains(f.getColumn("A")));
        assertTrue(f_c_deps.contains(t)); // Project column depends on the output (range) table operation

        // Range table does not depend on the incoming project columns and their input table
        List<Element> t_deps = t.getDependencies();
        assertTrue(!t_deps.contains(f_b));
        assertTrue(!t_deps.contains(f));

        // Population size
        assertEquals(5L, t.getLength());

        // Check links
        assertEquals(2L, f_b.getValue(6));
        assertEquals(4L, f_b.getValue(7));
        assertEquals(-1L, f_b.getValue(8));
    }

    Schema createSchemaDuration() { // For links to range table
        Schema s = new Schema("My Schema 3");

        //
        // Table 1 (range table)
        //
        Table t = s.createTable("R");

        Column t_a = s.createColumn("V", t);
        t_a.noop(true);
        Column t_i = s.createColumn("I", t);
        t_i.noop(true);

        t.range(
                Instant.parse("2018-01-01T00:45:00.00Z"),
                Duration.ofHours(2),
                2L
        );

        //
        // Table 2 (referencing table)
        //
        Table f = s.createTable("F");

        Column f_a = s.createColumn("A", f);

        // Add two records to link from
        f.add(8);
        f_a.setValue(0, Instant.parse("2017-01-01T00:00:00.00Z")); // Too low
        f_a.setValue(1, Instant.parse("2018-01-01T00:45:00.00Z")); // Exactly very first point
        f_a.setValue(2, Instant.parse("2018-01-01T01:46:00.00Z")); // Between first and second points
        f_a.setValue(3, Instant.parse("2018-01-01T02:45:00.00Z")); // Second and last point
        f_a.setValue(4, Instant.parse("2018-01-01T02:45:00.00Z")); // Last point
        f_a.setValue(5, Instant.parse("2018-01-01T03:45:00.00Z")); // After last point but within the last interval
        f_a.setValue(6, Instant.parse("2019-01-01T00:00:00.00Z")); // Too high
        f_a.setValue(7, null); // null

        Column f_b = s.createColumn("B", f, t);

        return s;
    }
}
