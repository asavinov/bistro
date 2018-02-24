package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void rangeNumberTest() {
        Schema s = createSchema();
        Table t = s.getTable("R");

        Column c11 = t.getColumn("V");
        Column c12 = t.getColumn("I");

        //
        // Define range table
        //
        t.range(
                10.0,
                20.0,
                5L
        );

        s.eval();

        // Check correctness of dependencies
        List<Element> t_deps = t.getDependencies();
        assertEquals(0, t_deps.size());

        // Result size
        assertEquals(5, t.getLength());

        // c11 = {111222}
        assertEquals(10.0, c11.getValue(0));
        assertEquals(90.0, c11.getValue(4));

        // c12 = {123123}
        assertEquals(0L, c12.getValue(0));
        assertEquals(4L, c12.getValue(4));
    }

    @Test
    public void rangeDurationTest() {
        Schema s = createSchema();
        Table t = s.getTable("R");

        Column c11 = t.getColumn("V");
        Column c12 = t.getColumn("I");

        //
        // Define range table
        //
        t.range(
                Instant.parse("2018-01-01T00:00:10.00Z"),
                Duration.ofSeconds(20),
                5L
        );

        s.eval();

        // Check correctness of dependencies
        List<Element> t_deps = t.getDependencies();
        assertEquals(0, t_deps.size());

        // Result size
        assertEquals(5, t.getLength());

        // c11 = {111222}
        assertEquals(Instant.parse("2018-01-01T00:00:10.00Z"), c11.getValue(0));
        assertEquals(Instant.parse("2018-01-01T00:01:30.00Z"), c11.getValue(4));

        // c12 = {123123}
        assertEquals(0L, c12.getValue(0));
        assertEquals(4L, c12.getValue(4));
    }

    Schema createSchema() {
        // Create and configure: schema, tables, keyColumns
        Schema s = new Schema("My Schema");

        //
        // Table
        //
        Table t = s.createTable("R");

        Column c11 = s.createColumn("V", t);
        c11.noop(true); // Range column

        Column c12 = s.createColumn("I", t);
        c12.noop(true); // Interval column

        return s;
    }

}
