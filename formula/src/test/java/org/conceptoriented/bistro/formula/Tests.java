package org.conceptoriented.bistro.formula;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.conceptoriented.bistro.core.*;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void calcTest() {
        Schema s = this.createCalcSchema();
        Column ta = s.getColumn("T", "A");
        Column tb = s.getColumn("T", "B");

        // Create expression from formula
        Formula expr = new FormulaExp4j("2 * [A] + 1", s.getTable("T"));

        // Define and evaluate
        tb.calculate( expr.getEvaluator(), expr.getParameterPaths().toArray(new ColumnPath[]{}) );
        tb.evaluate();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getData().getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getData().getValue(1));
        assertEquals(13.0, (Double) tb.getData().getValue(2), Double.MIN_VALUE);
    }

    Schema createCalcSchema() {
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        t.add();
        t.add();
        t.add();
        ta.getData().setValue(0, 5.0);
        ta.getData().setValue(1, null);
        ta.getData().setValue(2, 6.0);

        return s;
    }


    @Test
    public void linkTest() {
        Schema s = createLinkSchema();
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");
        Column t2c = t2.getColumn("C");

        // Create expressions: A=[A]; B=[B]
        Expression[] exprs = new Expression[] {
                new FormulaExp4j("[A]", t2),
                new FormulaExp4j("[B]", t2)
        };

        // Define and evaluate

        /* Deprecated. Now it is necessary to compute columns using expressions and then use them in links - direct use of expressions in links is not possible.
        t2c.link(
                exprs,
                t.getColumn("A"), t.getColumn("B")
        );
        t2c.evaluate();

        // Check correctness of dependencies
        List<Element> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(s.getColumn("T2", "A")));
        assertTrue(t2c_deps.contains(s.getColumn("T2", "B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(-1L, t2c.getValue(1)); // Not found
        */
    }

    Schema createLinkSchema() {
        // Create and configure: schema, tables, columns
        Schema s = new Schema("My Schema");

        //
        // Table 1 (type table to link to)
        //
        Table t1 = s.createTable("T");

        Column t1a = s.createColumn("A", t1);
        Column t1b = s.createColumn("B", t1);

        // Add one record to link to
        t1.add();
        t1a.getData().setValue(0, 5.0);
        t1b.getData().setValue(0, "bbb");

        //
        // Table 2 (referencing table to link records from)
        //
        Table t2 = s.createTable("T2");

        Column t2a = s.createColumn("A", t2);
        Column t2b = s.createColumn("B", t2);

        Column t2c = s.createColumn("C", t2, t1);

        // Add two records to link from
        t2.add();
        t2a.getData().setValue(0, 5.0);
        t2b.getData().setValue(0, "bbb");
        t2.add();
        t2a.getData().setValue(1, 10.0);
        t2b.getData().setValue(1, "ccc");

        return s;
    }


    @Test
    public void accuTest() {
        Schema s = this.createAccuSchema();
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = s.getColumn("T", "A");
        Column t2g = s.getColumn("T2", "G");

        t2g.evaluate();

        ta.getData().setDefaultValue(0.0);

        // Accu expression translated from a formula
        Expression expr = new FormulaExp4j(" [out] + 2.0 * [Id] ", s.getTable("T2"));

        ta.accumulate(
                t2g,
                (a,p) -> (Double)a + 2.0 * (Double)p[0],
                null,
                t2.getColumn("Id")
        );
        ta.evaluate();

        // Check correctness of dependencies
        List<Element> ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(s.getColumn("T2", "Id"))); // Used in formula
        assertTrue(ta_deps.contains(s.getColumn("T2", "G"))); // Group path

        assertEquals(20.0, ta.getData().getValue(0));
        assertEquals(20.0, ta.getData().getValue(1));
        assertEquals(0.0, ta.getData().getValue(2));

        // Test how dirty status is propagated through dependencies
        t2.getColumn("Id").getData().setValue(2, 5.0); // Change (make dirty) non-derived column
        s.evaluate(); // Both t2g and ta have to be evaluated

        assertEquals(30.0, ta.getData().getValue(0));
    }

    Schema createAccuSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t1 = schema.createTable("T");

        Column tid = schema.createColumn("Id", t1);

        // Define accu column
        Column ta = schema.createColumn("A", t1);

        t1.add();
        t1.add();
        t1.add();
        tid.getData().setValue(0, 5.0);
        tid.getData().setValue(1, 10.0);
        tid.getData().setValue(2, 15.0);

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("Id", t2);

        // Define group column
        Column t2g = schema.createColumn("G", t2, t1);
        Expression expr = new FormulaExp4j("[Id]", t2); // Deprecated. Expressions cannot be directly used in link column definitions - the columns have to be created explicitly
        t2g.link(
                new Column[] { t2id },
                t1.getColumn("Id")
        );

        t2.add();
        t2.add();
        t2.add();
        t2.add();
        t2id.getData().setValue(0, 5.0);
        t2id.getData().setValue(1, 5.0);
        t2id.getData().setValue(2, 10.0);
        t2id.getData().setValue(3, 20.0);

        return schema;
    }

}
