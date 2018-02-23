package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
        t2c.eval();

        // Check correctness of dependencies
        List<Element> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t2.getColumn("B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(-1L, t2c.getValue(1)); // Not found

        Expression[] valueExprs = new Expression[] {
                new Expr( p -> p[0], t2.getColumn("A") ), // This expression computers values for "A"
                new Expr( p -> p[0], t2.getColumn("B") ) // This expression computers values for "B"
        };

        t2c.proj(
                valueExprs
                // keyColumns - by default, if no key columns are specified then existing key columns will be used
        );
        t2c.eval();

        // Check correctness of dependencies
        t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t2.getColumn("B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(1L, t2c.getValue(1)); // New record append by proj-column
    }

    Schema createSchema() {
        Schema s = new Schema("My Schema");

        //
        // Table 1 (type table to link to)
        //
        Table t1 = s.createTable("T");
        t1.prod();

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

    @Test
    public void linkRangeTest() {
        Schema s = createSchema2();
        Table t = s.getTable("T"); // Group range table
        Table t2 = s.getTable("T2"); // Fact table

        Column t2a = t2.getColumn("A");

        //
        // Define link column
        //
        Column t2c = t2.getColumn("C");
        t2c.link(
                new ColumnPath(t2a) // This column values will be mapped to intervals
        );

        s.eval();

        // Check correctness of dependencies
        List<Element> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t));

        // Population size
        assertEquals(5L, t.getLength());

        // Check links
        assertEquals(-1L, t2c.getValue(0));
        assertEquals(0L, t2c.getValue(1));
        assertEquals(0L, t2c.getValue(2));
        assertEquals(1L, t2c.getValue(3));
        assertEquals(4L, t2c.getValue(4));
        assertEquals(4L, t2c.getValue(5)); // !!!
        assertEquals(-1L, t2c.getValue(6));
        assertEquals(-1L, t2c.getValue(7));
    }

    @Test
    public void projRangeTest() {
        Schema s = createSchema2();
        Table t = s.getTable("T"); // Group range table
        Table t2 = s.getTable("T2"); // Fact table

        // Modify definition to allow for more intervals
        t.range(
                10.0, // Origin: unchanged
                20.0, // Step: unchanged
                8L // Count: 7 instead of 5
        );

        // Add more facts to be mapped to non-existing intervals
        Column t2a = t2.getColumn("A");
        t2a.setValue(6, 110.0); // 1 interval has to be added [110,130)
        t2a.setValue(7, 160.0); // 2 intervals have to be added [130,150) and [150,170)
        t2.add();
        t2a.setValue(8, 260.0); // No intervals have to be added because exceeds the maximum count

        //
        // Define proj column
        //
        Column t2c = t2.getColumn("C");
        t2c.proj(
                new ColumnPath(t2.getColumn("A")) // This column values will be mapped to intervals
        );

        s.eval();

        // Check correctness of dependencies
        List<Element> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(!t2c_deps.contains(t)); // Proj column does not depend on the range table

        // Range table depends on the incoming proj-column and its input table
        List<Element> t_deps = t.getDependencies();
        assertTrue(t_deps.contains(t2c));
        assertTrue(t_deps.contains(t2));

        // Population size
        assertEquals(8L, t.getLength());

        // Check links
        assertEquals(5L, t2c.getValue(6));
        assertEquals(7L, t2c.getValue(7));
        assertEquals(-1L, t2c.getValue(8));
    }

    Schema createSchema2() { // For links to range table
        Schema s = new Schema("My Schema 2");

        //
        // Table 1 (range table)
        //
        Table t1 = s.createTable("T");

        Column t1a = s.createColumn("A", t1);
        t1a.noop(true);
        Column t1b = s.createColumn("B", t1);
        t1b.noop(true);

        t1.range(
                10.0, // Origin of the raster
                20.0, // Step of the reaster (interval length)
                5L // How many intervals in the raster
        );

        //
        // Table 2 (referencing table)
        //
        Table t2 = s.createTable("T2");

        Column t2a = s.createColumn("A", t2);
        Column t2b = s.createColumn("B", t2);

        Column t2c = s.createColumn("C", t2, t1);

        // Add two records to link from
        t2.add(8);
        t2a.setValue(0, -200.0); // Too low
        t2a.setValue(1, 10.0); // Exactly very first point
        t2a.setValue(2, 20.0); // Between first and second points
        t2a.setValue(3, 30.0); // Second point
        t2a.setValue(4, 90.0); // Last point
        t2a.setValue(5, 100.0); // After last point but within the last interval
        t2a.setValue(6, 200.0); // Too high
        t2a.setValue(7, Double.NaN); // NaN

        return s;
    }

}
