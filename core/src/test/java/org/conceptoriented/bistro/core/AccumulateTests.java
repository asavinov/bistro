package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccumulateTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    Schema createSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t = schema.createTable("T");

        Column tid = schema.createColumn("Id", t);
        tid.noop(true);

        // Define accumulate column
        Column ta = schema.createColumn("A", t);

        t.add();
        t.add();
        t.add();
        tid.setValue(0, 5);
        tid.setValue(1, 10);
        tid.setValue(2, 15);

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("Id", t2);

        // Define group link column: G: T2 -> T (where T2:Id = T:Id)
        Column t2g = schema.createColumn("G", t2, t);
        t2g.link(
                new Column[] { t2id }, // Values: Id from T2
                new Column[] { tid } // Keys: Id from T
        );

        t2.add();
        t2.add();
        t2.add();
        t2.add();
        t2id.setValue(0, 5);
        t2id.setValue(1, 5);
        t2id.setValue(2, 10);
        t2id.setValue(3, 20);

        return schema;
    }

    @Test
    public void accuTest() {
        Schema s = this.createSchema();
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");

        // Group link column
        Column t2g = t2.getColumn("G");
        Column t2id = t2.getColumn("Id");

        // Accumulate column
        // Lambda for accumulation " [out] + [Id] "
        Column ta = t.getColumn("A");
        ta.setDefaultValue(0.0);
        ta.accumulate(
                t2g,
                (a,p) -> ((Number)a).doubleValue() + ((Number)p[0]).doubleValue(),
                (a,p) -> ((Number)a).doubleValue() - ((Number)p[0]).doubleValue(),
                t2.getColumn("Id")
        );

        s.evaluate();

        // Check correctness of dependencies
        List<Element> ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(t2.getColumn("Id"))); // Used in formula
        assertTrue(ta_deps.contains(t2.getColumn("G"))); // Group path

        assertEquals(10.0, ta.getValue(0));
        assertEquals(10.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));

        //
        // Test incremental evaluation using adder
        //

        t2.add();
        t2id.setValue(4, 10);

        s.evaluate();

        assertEquals(10.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));

        t2.remove();

        s.evaluate();

        assertEquals(5.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));

        t2.remove();
        t2.remove();
        t2.add();
        t2id.setValue(5, 15);

        s.evaluate();

        assertEquals(0.0, ta.getValue(0));
        assertEquals(10.0, ta.getValue(1));
        assertEquals(15.0, ta.getValue(2));

        t2.remove(3);

        s.evaluate();

        assertEquals(0.0, ta.getValue(0));
        assertEquals(0.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }

}
