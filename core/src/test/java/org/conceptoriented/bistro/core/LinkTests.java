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
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");
        Column t2c = t2.getColumn("C");

        // Use column valuePaths
        Column valueColumn = t2.getColumn("A");

        t2c.link(
                new ColumnPath(valueColumn)
        );

        s.eval();

        // Check correctness of dependencies
        List<Element> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t));

        // Range table has to be populated
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
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");
        Column t2c = t2.getColumn("C");

        // Use column valuePaths
        Column valueColumn = t2.getColumn("A");

        t2c.proj(
                new ColumnPath(valueColumn)
        );

        s.eval();

        // Check correctness of dependencies

        // TODO: We changed def of the link columns (to proj), and the range table must be marked dirty and re-populated,
        // because the incoming proj-columns is considered a constraint, that is, it will essentially provide records (old population is invalid now)
        // So we need to check that this dependency really works and range table will be emptied (not populated because it canot be populated itself) before proj-column evaluation.

        // TODO: We need to implement convenience methods of range table for working with intervals like
        // find(value) - find interval for this value (if any) as its representative raster point or as two values (borders)
        // getLeft/getRight(i) or getStart(i)/getEnd(i) (return either values or indexes) which will return indexes like [i,i+1) depending on options
        // These should be specific only to range tables, so maybe implement them in Definition (which is essentially treated as a Column subclass with a field referencing its super-object)..
        // Use cases:
        // - in link-column, if we had a high value then we had to determine whether it is too high, or it still belongs to the last interval
        //   We had to do interval arithmetics which is wrong -instead, we have to call some method which determines whether this value belongs to some interval (i-th interval or whatever).

        // TODO: Update DefinitionProj validate() method accordingly to work with range target (maybe add validate for DefinitionLink and other defs)

        // TODO: The whole logic of proj depends on the isProj flag and is implemented in the find method of the table class.
        // In the case of range tables, we use findRange. However, it is unaware of intervals.
        // So we either we implement the interval-specific part ourselves, e.g,. find method returns only insert-point and we then decide what to insert and how to insert (interval-specific).
        // Or this find method is made interval-aware, that is, essentially a method of a range table, e.g., implemented in range table definition.
        // Note that currrently it is already interval-aware. Maybe rename it to findInterval or findInRange.
        // Note also that it will have to also work with Date ranges, so we need some kind of generic logic of procesing either in one generic method or in two implementations.

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

        t1.range(10.0, 20.0, 5L);

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
        t2a.setValue(5, 95.0); // After last point but within interval
        t2a.setValue(6, 200.0); // Too high
        t2a.setValue(7, Double.NaN); // NaN

        return s;
    }

}
