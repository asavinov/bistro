package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccuTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void accuTest() {
        Schema s = this.createSchema();
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = t.getColumn("A");
        Column t2g = t2.getColumn("G");

        t2g.evaluate();

        // Lambda for accumulation " [out] + 2.0 * [Id] "
        ta.setDefaultValue(0.0);
        ta.accu(
                t2g,
                (a,p) -> 2.0 * (Double)p[0] + (Double)a,
                t2.getColumn("Id")
        );
        ta.evaluate();

        // Check correctness of dependencies
        List<Element> ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(t2.getColumn("Id"))); // Used in formula
        assertTrue(ta_deps.contains(t2.getColumn("G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }

    Schema createSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t = schema.createTable("T");

        Column tid = schema.createColumn("Id", t);
        tid.noop(true);

        // Define accu column
        Column ta = schema.createColumn("A", t);
        ta.setDefinitionType(ColumnDefinitionType.ACCU);

        t.add();
        t.add();
        t.add();
        tid.setValue(0, 5.0);
        tid.setValue(1, 10.0);
        tid.setValue(2, 15.0);

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("Id", t2);

        // Define group column: G: T2 -> T
        Column t2g = schema.createColumn("G", t2, t);
        t2g.link(
                new Column[] { t2id }, // Values: Id from T2
                new Column[] { tid } // Keys: Id from T
        );

        t2.add();
        t2.add();
        t2.add();
        t2.add();
        t2id.setValue(0, 5.0);
        t2id.setValue(1, 5.0);
        t2id.setValue(2, 10.0);
        t2id.setValue(3, 20.0);

        return schema;
    }

}
